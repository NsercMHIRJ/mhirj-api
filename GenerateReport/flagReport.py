from GenerateReport.helper import LongestConseq, check_2in5, connect_database_MDCdata, connect_database_MDCmessagesInputs, connect_database_TopMessagesSheet, connect_to_fetch_all_jam_messages, highlightJams, isValidParams
import numpy as np
import pandas as pd
from GenerateReport.history import historyReport
import re
# pd.set_option('display.max_rows', None)



def mdcDF(MaxAllowedOccurrences: int, MaxAllowedConsecLegs: int, MaxAllowedIntermittent: int, MaxAllowedConsecDays: int, ata: str, exclude_EqID:str, airline_operator: str, include_current_message: int, fromDate: str , toDate: str):
        MDCdataDF = connect_database_MDCdata(ata, exclude_EqID, airline_operator, include_current_message, fromDate, toDate)
        MDCdataDF["MSG_Date"] = pd.to_datetime(MDCdataDF["MSG_Date"]) # formatting for date
        MDCdataDF["FLIGHT_LEG"].fillna(value= 0.0, inplace= True) # Null values preprocessing - if 0 = Currentflightphase
        MDCdataDF["FLIGHT_PHASE"].fillna(False, inplace= True) # NuCell values preprocessing for currentflightphase
        MDCdataDF["INTERMITNT"].fillna(value= 0.0, inplace= True) # Null values preprocessing for currentflightphase
        MDCdataDF["INTERMITNT"].replace(to_replace= ">", value= "9", inplace=True) # > represents greater than 8 Intermittent values
        MDCdataDF["AC_SN"] = MDCdataDF["AC_SN"].str.replace('AC', '')
        MDCdataDF.fillna(value= " ", inplace= True) # replacing all REMAINING null values to a blank string
        MDCdataDF.sort_values(by= "MSG_Date", ascending= False, inplace= True, ignore_index= True)
        return MDCdataDF

Flagsreport = 1 # this is here to initialize the variable. user must start report by choosing Newreport = True
def Toreport(Flagsreport, HistoryReport,MDCdataDF,include_current_message,list_of_tuples_acsn_bcode,newreport):
    '''Populates a report with input from the previous report, aircraft serial number and B1 message code'''
    
    if newreport:
        del Flagsreport
        Flagsreport = pd.DataFrame(data= None)
    indexedreport = HistoryReport.set_index(["AC SN", "B1-Equation"])
    print("--------indexdreport-------------")
    print(indexedreport)
    
    #creating dataframe to look at dates
    if (include_current_message == 1):
    # if CurrentFlightPhaseEnabled == 1: #Show all, current and history
        DatesDF = MDCdataDF[["MSG_Date","EQ_ID", "AC_SN"]].copy()
        print("-----this is datadf-----")
        print(DatesDF)

    elif (include_current_message == 0):
    # elif CurrentFlightPhaseEnabled == 0: #Only show history
        DatesDF = MDCdataDF[["MSG_Date","EQ_ID", "AC_SN", "FLIGHT_PHASE"]].copy()
        print("-----this is datadf---2-----")
        print(DatesDF)
        DatesDF = DatesDF.replace(False, np.nan).dropna(axis=0, how='any')
        print("-----this is datadf---3-----")
        print(DatesDF)
        DatesDF = DatesDF[["MSG_Date","EQ_ID", "AC_SN"]].copy()
        print("-----this is datadf---4-----")
        print(DatesDF)

    # this exists to check which dates are present for the specific aircraft and message chosen
    counts = pd.DataFrame(data= DatesDF.groupby(['AC_SN', "EQ_ID", "MSG_Date"]).agg(len), columns= ["Counts"])
    counts
    print("--------counts----------")
    print(counts)

    
    DatesfoundinMDCdata = ''
    unavailable = []
    for ab in list_of_tuples_acsn_bcode.split('),'):
                removeParanthesisRegex2 = r"[()]"
                removeBraces = re.sub(removeParanthesisRegex2, '', ab)
                values = removeBraces.split(',')  
                values=  [value.replace("'", '') for value in values]
                AircraftSN = values[0]
                Bcode= values[1]
                print(AircraftSN)
                print(Bcode)
                
                try:
                     DatesfoundinMDCdata = counts.loc[(AircraftSN, Bcode)].resample('D')["Counts"].sum().index
                     print("------------datesfound in mdc---------")
                     print(DatesfoundinMDCdata)
                except:
                     unavailable.append(ab)
                     print("------exception----------")

                # newrow = indexedreport.loc[(AircraftSN, Bcode), ["AC_TN", "ATA", "LRU", "MDC Message", "Type","EICAS Message", "MEL or No-Dispatch", "MHIRJ Input", "MHIRJ Recommendation", "Additional Comments"]].to_frame().transpose()
                newrow = indexedreport.loc[(AircraftSN, Bcode), ["AC_TN", "ATA", "LRU", "Type","EICAS Message", "MEL or No-Dispatch", "MHIRJ Input", "MHIRJ Recommendation", "Additional Comments"]].to_frame().transpose()
                print("--this is new row---")
                print(newrow)
                newrow.insert(loc= 0, column= "AC SN", value= AircraftSN)
                newrow.insert(loc= 3, column= "B1-code", value= Bcode)
                newrow.insert(loc= 8, column= "Date From", value= DatesfoundinMDCdata.min().date()) #.date()removes the time data from datetime format
                newrow.insert(loc= 9, column= "Date To", value= DatesfoundinMDCdata.max().date())
                newrow.insert(loc= 10, column= "SKW action WIP", value= "")
                # newrow = newrow.rename(columns= {"AC SN":"MSN", "MDC Message": "Message", "EICAS Message":"Potential FDE"})
                newrow = newrow.rename(columns= {"AC SN":"MSN", "EICAS Message":"Potential FDE"})

                # append the new row to the existing report
                Flagsreport = Flagsreport.append(newrow, ignore_index= True)

    return Flagsreport    
