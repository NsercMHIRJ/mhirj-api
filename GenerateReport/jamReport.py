from GenerateReport.helper import LongestConseq, check_2in5, connect_database_MDCdata, connect_database_MDCmessagesInputs, connect_database_TopMessagesSheet, connect_to_fetch_all_jam_messages, highlightJams, isValidParams,connect_database_MDCmessagesInputs_jamReport
import numpy as np
import pandas as pd
from GenerateReport.history import historyReport
# pd.set_option("display.max_rows", None, "display.max_columns", None)


def mdcDF(MaxAllowedOccurrences: int, MaxAllowedConsecLegs: int, MaxAllowedIntermittent: int, MaxAllowedConsecDays: int, ata: str, exclude_EqID:str, airline_operator: str, include_current_message: int, aircraft_no: str, fromDate: str , toDate: str):
    MDCdataDF = connect_database_MDCdata( aircraft_no,ata, exclude_EqID, airline_operator, include_current_message, fromDate, toDate)
    MDCdataDF["AC_SN"] = MDCdataDF["AC_SN"].str.replace('AC', '')
    MDCdataDF.fillna(value= " ", inplace= True) # replacing all REMAINING null values to a blank string
    MDCdataDF.sort_values(by= "MSG_Date", ascending= False, inplace= True, ignore_index= True)
    return MDCdataDF

# this list can be updated to include other messages
listofJamMessages = ["B1-309178","B1-309179","B1-309180","B1-060044","B1-060045","B1-007973",
                     "B1-060017","B1-006551","B1-240885","B1-006552","B1-006553","B1-006554",
                     "B1-006555","B1-007798","B1-007772","B1-240938","B1-007925","B1-007905",
                     "B1-007927","B1-007915","B1-007926","B1-007910","B1-007928","B1-007920"] 


 
def jamReport(OutputTable, ACSN_chosen,MDCdataDF,listofmessages= listofJamMessages):

#    listofJamMessages = list()
#    all_jam_messages = connect_to_fetch_all_jam_messages()
#    for each_jam_message in all_jam_messages['Jam_Message']:
#        listofJamMessages.append(each_jam_message)
   inputMessage=connect_database_MDCmessagesInputs_jamReport()
   print("--------inputmsg------")
   print(inputMessage)
   datatofilter = MDCdataDF.copy(deep=True)
   print("------dataoffilter-----")
   print(datatofilter)
   print("---b1--------")
   
   test_jam = OutputTable.loc[OutputTable['AC SN'] == str(ACSN_chosen),'is_jam'].values[0]
   print(test_jam)

   if test_jam:
   
        isin = OutputTable["B1-Equation"].isin(listofmessages)
        print("------isin----------")
        print(isin)

   else :  
        isin = ~OutputTable["B1-Equation"].isin(listofmessages)
        print("------isin----------")
        print(isin) 


 
   filter1 = OutputTable[isin][["AC SN", "B1-Equation"]]
   print("----filter---------------")
   print(filter1)
   listoftuplesACID = list(zip(filter1["AC SN"], filter1["B1-Equation"]))
   print("----listoftuplesAcid-----------")
   print(listoftuplesACID)
 
   datatofilter2 = datatofilter.set_index(["AC_SN", "EQ_ID"]).sort_index().loc[
                           pd.IndexSlice[listoftuplesACID], :].reset_index()
   listoftuplesACFL = list(zip(datatofilter2["AC_SN"], datatofilter2["FLIGHT_LEG"]))
   datatofilter3 = datatofilter.set_index(["AC_SN", "FLIGHT_LEG"]).sort_index()
   print("-------datafilter3------")
   print(datatofilter3)
   FinalDF = datatofilter3.loc[pd.IndexSlice[listoftuplesACFL], :]
   print("----------finaldf---------------")
   print(FinalDF)
   FinalDF_history = (FinalDF.loc[str(ACSN_chosen)])
   print("--------json jam-----------")
   print(FinalDF_history)
#    print (FinalDF_history_json.reset_index().to_json(orient='records'))
   FinalDF_history_json = FinalDF_history.reset_index()
   print("---final history---------")
   print(FinalDF_history_json)
  
#    dfinal = df1.merge(df2, how='inner', left_on='movie_title', right_on='movie_name')

   merged_df = FinalDF_history_json.merge(inputMessage, how='inner', left_on='EQ_ID', right_on='Equation_ID')
   print("---------merged df-----------")
   print(merged_df)

   return merged_df
