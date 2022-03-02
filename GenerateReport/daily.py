from GenerateReport.helper import LongestConseq, check_2in5, connect_database_MDCdata, connect_database_MDCmessagesInputs, connect_database_TopMessagesSheet, connect_to_fetch_all_jam_messages, highlightJams, isValidParams
import numpy as np
import pandas as pd
import numbers
def dailyReport(MaxAllowedOccurrences: int, MaxAllowedConsecLegs: int, MaxAllowedIntermittent: int, MaxAllowedConsecDays: int, ata: str, exclude_EqID:str, airline_operator: str, include_current_message: int, fromDate: str , toDate: str):
    MDCdataDF = connect_database_MDCdata(ata, exclude_EqID, airline_operator, include_current_message, fromDate, toDate)
    print("data ", MDCdataDF)
    # Date formatting
    MDCdataDF["MSG_Date"] = pd.to_datetime(MDCdataDF["MSG_Date"])
    # print(MDCdataDF["MSG_Date"])
    MDCdataDF["FLIGHT_LEG"].fillna(value=0.0, inplace=True)  # Null values preprocessing - if 0 = Currentflightphase
    MDCdataDF["FLIGHT_LEG"] = MDCdataDF["FLIGHT_LEG"].astype(int)
    MDCdataDF["FLIGHT_PHASE"].fillna(False, inplace=True)  # NuCell values preprocessing for currentflightphase
    MDCdataDF["INTERMITNT"].fillna(value=-1, inplace=True)  # Null values preprocessing for currentflightphase
    MDCdataDF["INTERMITNT"].replace(to_replace=">", value=9,
                                      inplace=True)  # > represents greater than 8 INTERMITNT values

    try:                      
        #print("data in intermittent ",MDCdataDF["INTERMITNT"])            
        MDCdataDF["INTERMITNT"] = int(MDCdataDF["INTERMITNT"]) # cast type to int
    except:
        #print("data in intermittent exec",MDCdataDF["INTERMITNT"])            
        MDCdataDF["INTERMITNT"] = 9


    MDCdataDF["AC_SN"] = MDCdataDF["AC_SN"].str.replace('AC', '')
    MDCdataDF.fillna(value=" ", inplace=True)  # replacing all REMAINING null values to a blank string
    MDCdataDF.sort_values(by= "MSG_Date", ascending= False, inplace= True, ignore_index= True)

    AircraftTailPairDF = MDCdataDF[["AC_SN", "AC_TN"]].drop_duplicates(ignore_index= True) # unique pairs of AC SN and AC_TN for use in analysis
    AircraftTailPairDF.columns = ["AC SN","AC_TN"] # re naming the columns to match History/Daily analysis output

    DatesinData = MDCdataDF["MSG_Date"].dt.date.unique()  # these are the dates in the data in Datetime format.
    NumberofDays = len(MDCdataDF["MSG_Date"].dt.date.unique())  # to pass into Daily analysis number of days in data
    latestDay = str(MDCdataDF.loc[0, "MSG_Date"].date())  # to pass into Daily analysis
    LastLeg = max(MDCdataDF["FLIGHT_LEG"])  # Latest Leg in the data
    MDCdataArray = MDCdataDF.to_numpy()  # converting to numpy to work with arrays

    # MDCMessagesDF = pd.read_csv(MDCMessagesURL_path, encoding="utf8")  # bring messages and inputs into a Dataframe
    MDCMessagesDF = connect_database_MDCmessagesInputs()
    MDCMessagesArray = MDCMessagesDF.to_numpy()  # converting to numpy to work with arrays
    ShapeMDCMessagesArray = MDCMessagesArray.shape  # tuple of the shape of the MDC message data (#rows, #columns)

    # TopMessagesDF = pd.read_csv(TopMessagesURL_path)  # bring messages and inputs into a Dataframe
    TopMessagesDF = connect_database_TopMessagesSheet()
    TopMessagesArray = TopMessagesDF.to_numpy()  # converting to numpy to work with arrays


    UniqueSerialNumArray = []

    if(airline_operator.upper() == "SKW"):
        airline_id = 101
    Flagsreport = 1  # this is here to initialize the variable. user must start report by choosing Newreport = True
    # Flagsreport: int, AircraftSN: str, , newreport: int, CurrentFlightPhaseEnabled: int
    if(include_current_message == 1):
        CurrentFlightPhaseEnabled = 1
    else:
        CurrentFlightPhaseEnabled = 0  # 1 or 0, 1 includes current phase, 0 does not include current phase

    # MaxAllowedOccurances = occurences  # flag for Total number of occurences -> T
    # MaxAllowedConsecLegs = legs  # flag for consecutive legs -> CF
    # MaxAllowedConsecDays = consecutiveDays
    if MaxAllowedIntermittent > 9:
        MaxAllowedIntermittent = 9  # flag for intermittent values ->IM
    
    #Bcode = equationID
    newreport = True    #set a counter variable to bring back it to false

    #global UniqueSerialNumArray

    # function to separate the chunks of data and convert it into a numpy array
    def separate_data(data, date):
        '''Takes data as a dataframe, along with a date to slice the larger data to only include the data in that date'''

        DailyDataDF = data.loc[date]
        return DailyDataDF

    AnalysisDF = MDCdataDF.set_index(
        "MSG_Date")  # since MSG_Date was moved to the index of the DF, the column values change from the original MDCdataDF


    currentRow = 0
    MAINtable_array_temp = np.empty((1,20),object) # 18 for the date 
    MAINtable_array = []

    for i in range(0, NumberofDays):
        
        daytopass = str(DatesinData[i])
        #define array to analyze
        DailyanalysisDF = separate_data(AnalysisDF, daytopass)
        
        # total occurrences
        if include_current_message == 1:
            # include the current flight legs
            selection = DailyanalysisDF[["EQ_ID", "AC_SN"]]
            total_occ_DF = selection.value_counts().unstack()

            # selecting only the necessary data for - consecutive legs
            legs_selection = DailyanalysisDF[["AC_SN", "EQ_ID", "FLIGHT_LEG"]].sort_values(by= ["FLIGHT_LEG"], ascending= False).copy()
            legs_selection.set_index(["AC_SN", "EQ_ID"], inplace= True)
            legs_selection.sort_index(inplace= True)

            # selecting only the necessary data for - intermittent
            Intermittent_selection = DailyanalysisDF[["AC_SN", "EQ_ID", "INTERMITNT"]].copy()
            Intermittent_selection.set_index(["AC_SN", "EQ_ID"], inplace= True)
            Intermittent_selection.sort_index(inplace= True)

        elif include_current_message == 0:
            # exclude the current flight legs
            selection = DailyanalysisDF[["EQ_ID", "AC_SN", "FLIGHT_LEG"]][DailyanalysisDF["FLIGHT_LEG"] != 0].copy()
            selection.drop(["FLIGHT_LEG"], inplace= True, axis= 1)
            total_occ_DF = selection.value_counts().unstack()

            # selecting only the necessary data for - consecutive legs
            legs_selection = DailyanalysisDF[["AC_SN", "EQ_ID", "FLIGHT_LEG"]][DailyanalysisDF["FLIGHT_LEG"] != 0].sort_values(by= ["FLIGHT_LEG"], ascending= False).copy()
            legs_selection.set_index(["AC_SN", "EQ_ID"], inplace= True)
            legs_selection.sort_index(inplace= True)

            # selecting only the necessary data for - intermittent
            Intermittent_selection = DailyanalysisDF[["AC_SN", "EQ_ID", "INTERMITNT", "FLIGHT_LEG"]][DailyanalysisDF["FLIGHT_LEG"] != 0].copy()
            Intermittent_selection.drop(["FLIGHT_LEG"], inplace= True, axis= 1)
            Intermittent_selection.set_index(["AC_SN", "EQ_ID"], inplace= True)
            Intermittent_selection.sort_index(inplace= True)

        # obtain location of nonempty values
        b1message, aircraft = np.where(pd.notnull(total_occ_DF))
        # obtains the address to the values to be referenced later
        notEmptyLabelPairs = np.column_stack([total_occ_DF.columns[aircraft],total_occ_DF.index[b1message]])

        # creating a dataframe with a similar size to total occurrences df
        consec_legs = pd.DataFrame().reindex_like(total_occ_DF)

        # creating a dataframe with a similar size to total occurrences df
        max_intermittent = pd.DataFrame().reindex_like(total_occ_DF)

        # creating a dataframe with a similar size to total occurrences df
        flags_array = pd.DataFrame().reindex_like(total_occ_DF)

        # fill null values with 0
        consec_legs.fillna(value= 0.0, inplace = True)
        max_intermittent.fillna(value= 0.0, inplace= True)
        total_occ_DF.fillna(value= 0.0, inplace= True)
        flags_array.fillna(value= "", inplace= True)

        # Creating flags lists
        flags_jams = MDCMessagesDF.loc[(MDCMessagesDF["Occurrence_Flag"] == 1) & (MDCMessagesDF["Days_Count"] == 0)]["Equation_ID"]

        # go through AC/eqnID combinations for analysis
        for i in range(len(notEmptyLabelPairs)):

            # pick AC and eqn ID combo
            aircraft = notEmptyLabelPairs[i, 0]
            equation = notEmptyLabelPairs[i, 1]

            # set up legs selection for analysis
            legs = legs_selection.loc[aircraft, equation]
            if len(legs) > 1:
                legs = legs["FLIGHT_LEG"].unique()
                consec_legs.at[equation, aircraft] = LongestConseq(unique_arr= legs, days_legs= "legs")
                try:
                    if all(v.strip() for v in legs) and (legs > 32600).any():
                        legs_flag = True
                    else:
                        legs_flag = False
                except:
                    legs_flag = False
                    
            elif len(legs) == 1:
                legs = legs["FLIGHT_LEG"].item()
                consec_legs.at[equation, aircraft] = 0
                
                if int(legs) > 32600:
                    legs_flag = True
                else:
                    legs_flag = False

            # set up intermitt selection for analysis
            intermitt = Intermittent_selection.loc[aircraft, equation]

            # run
            
            if len(intermitt) > 1:
                max_intermittent.at[equation, aircraft] = max(intermitt["INTERMITNT"])
            elif len(intermitt) == 1:
                max_intermittent.at[equation, aircraft] = intermitt["INTERMITNT"].item()
                
            if total_occ_DF.at[equation, aircraft] >= MaxAllowedOccurrences \
            or consec_legs.at[equation, aircraft] >= MaxAllowedConsecLegs \
            or max_intermittent.at[equation, aircraft] >= MaxAllowedIntermittent \
            or legs_flag \
            or (flags_jams == equation).any():

                if total_occ_DF.at[equation, aircraft] >= MaxAllowedOccurrences:
                    flags_array.at[equation, aircraft] = flags_array.at[equation, aircraft] + "Total occurrences exceeded " + str(MaxAllowedOccurrences) + " occurrences. "

                if consec_legs.at[equation, aircraft] >= MaxAllowedConsecLegs:
                    flags_array.at[equation, aircraft] = flags_array.at[equation, aircraft] + "Maximum consecutive flight legs exceeded " + str(MaxAllowedConsecLegs) + " flight legs. "

                if max_intermittent.at[equation, aircraft] >= MaxAllowedIntermittent:
                    flags_array.at[equation, aircraft] = flags_array.at[equation, aircraft] + "Maximum intermittent occurrences for one flight leg exceeded " + str(MaxAllowedIntermittent) + " occurrences. "

                if legs_flag:
                    flags_array.at[equation, aircraft] = flags_array.at[equation, aircraft] + "Flight legs above 32600. "

                if (flags_jams == equation).any():
                    flags_array.at[equation, aircraft] = flags_array.at[equation, aircraft] + str(equation) + " occurred at least once. "

                #populating the final array (Table)
                MAINtable_array_temp[0,0] = daytopass
                MAINtable_array_temp[0,1] = aircraft # SN
                try:
                    MAINtable_array_temp[0,2] = MDCMessagesDF["EICAS"][MDCMessagesDF["Equation_ID"] == equation].item() #EICAS
                except: 
                    MAINtable_array_temp[0,2] = "No Data"
                
                try:
                    MAINtable_array_temp[0,3] = MDCMessagesDF["Message"][MDCMessagesDF["Equation_ID"] == equation].item() #Message
                except:
                    MAINtable_array_temp[0,3] = "No Data" #Message

                try:
                    MAINtable_array_temp[0,4] = MDCMessagesDF["LRU"][MDCMessagesDF["Equation_ID"] == equation].item() #LRU
                except:
                    MAINtable_array_temp[0,4] = "No Data"
                try:
                    MAINtable_array_temp[0,5] = MDCMessagesDF["ATA"][MDCMessagesDF["Equation_ID"] == equation].item() #ATA
                except: 
                    MAINtable_array_temp[0,5] = "No Data"
                MAINtable_array_temp[0,6] = equation #Eqn ID
                try:
                    MAINtable_array_temp[0,7] = MDCMessagesDF["Message Type"][MDCMessagesDF["Equation_ID"] == equation].item()
                except: 
                    MAINtable_array_temp[0,7] = "No Data"

                try:
                    MAINtable_array_temp[0,8] = MDCMessagesDF["Equation Description"][MDCMessagesDF["Equation_ID"] == equation].item()
                except: 
                    MAINtable_array_temp[0,8] = "No Data"
                MAINtable_array_temp[0,9] = total_occ_DF.at[equation, aircraft]
                
                MAINtable_array_temp[0,10] = consec_legs.at[equation, aircraft]
                MAINtable_array_temp[0,11] = max_intermittent.at[equation, aircraft]
                MAINtable_array_temp[0,12] = str(flags_array.at[equation, aircraft])

                try:
                    #if the input is empty set the priority to 4
                    if MDCMessagesDF["Priority"][MDCMessagesDF["Equation_ID"] == equation].item() == 0:
                        MAINtable_array_temp[0,13] = 4
                    else:
                        MAINtable_array_temp[0,13] = MDCMessagesDF["Priority"][MDCMessagesDF["Equation_ID"] == equation].item()
                except: 
                    MAINtable_array_temp[0,13] = 4

                #For B1-006424 & B1-006430 Could MDC Trend tool assign Priority 3 if logged on A/C below 10340, 15317. Priority 1 if logged on 10340, 15317, 19001 and up
                if equation == "B1-006424" or equation == "B1-006430":
                    if int(aircraft) <= 10340 and int(aircraft) > 10000:
                        MAINtable_array_temp[0,13] = 3
                    elif int(aircraft) > 10340 and int(aircraft) < 11000:
                        MAINtable_array_temp[0,13] = 1
                    elif int(aircraft) <= 15317 and int(aircraft) > 15000:
                        MAINtable_array_temp[0,13] = 3
                    elif int(aircraft) > 15317 and int(aircraft) < 16000:
                        MAINtable_array_temp[0,13] = 1
                    elif int(aircraft) >= 19001 and int(aircraft) < 20000:
                        MAINtable_array_temp[0,13] = 1

                try:
                    # #check the content of MHIRJ ISE recommendation and add to array    
                    if MDCMessagesDF["MEL_or_No_Dispatch"][MDCMessagesDF["Equation_ID"] == equation].item() == "0":
                        MAINtable_array_temp[0,16] = ""
                    else:
                        MAINtable_array_temp[0,16] = MDCMessagesDF["MEL_or_No_Dispatch"][MDCMessagesDF["Equation_ID"] == equation].item()
                except:
                    MAINtable_array_temp[0,16] = ""

                try:
                    #check content of "MHIRJ Input"
                    if MDCMessagesDF["MHIRJ_ISE_inputs"][MDCMessagesDF["Equation_ID"] == equation].item() == "0":
                        MAINtable_array_temp[0,15] = ""
                    else:
                        MAINtable_array_temp[0,15] = MDCMessagesDF["MHIRJ_ISE_inputs"][MDCMessagesDF["Equation_ID"] == equation].item()
                except:
                    MAINtable_array_temp[0,15] = ""
                
                try:
                    #check the content of MHIRJ ISE recommendation and add to array    
                    if MDCMessagesDF["MHIRJ_ISE_Recommended_Action"][MDCMessagesDF["Equation_ID"] == equation].item() == "0":
                        MAINtable_array_temp[0,17] = ""
                    else:
                        MAINtable_array_temp[0,17] = MDCMessagesDF["MHIRJ_ISE_Recommended_Action"][MDCMessagesDF["Equation_ID"] == equation].item()
                except:
                    MAINtable_array_temp[0,17] = ""

                try:
                    #check content of "additional"
                    if MDCMessagesDF["Additional_Comments"][MDCMessagesDF["Equation_ID"] == equation].item() == "0":
                        MAINtable_array_temp[0,18] = ""
                    else:
                        MAINtable_array_temp[0,18] = MDCMessagesDF["Additional_Comments"][MDCMessagesDF["Equation_ID"] == equation].item()
                except:
                    MAINtable_array_temp[0,18] = ""

                #Keywords
                try:
                    if MDCMessagesDF["Keywords"][MDCMessagesDF["Equation_ID"] == equation].item() == "0":
                        MAINtable_array_temp[0,19] = ""
                    else:
                        MAINtable_array_temp[0,19] = MDCMessagesDF["Keywords"][MDCMessagesDF["Equation_ID"] == equation].item()
                except:
                    MAINtable_array_temp[0,19] = ""

                #Check for the equation in the Top Messages sheet
                TopCounter = 0
                Top_LastRow = TopMessagesArray.shape[0]
                while TopCounter < Top_LastRow:

                    #Look for the flagged equation in the Top Messages Sheet
                    if equation == TopMessagesArray[TopCounter,4]:

                        #Found the equation in the Top Messages Sheet. Put the information in the report
                        MAINtable_array_temp[0,14] = "Known Nuissance: " + str(TopMessagesArray[TopCounter, 13]) \
                        + " / In-Service Document: " + str(TopMessagesArray[TopCounter,11]) \
                        + " / FIM Task: " + str(TopMessagesArray[TopCounter,10]) \
                        + " / Remarks: " + str(TopMessagesArray[TopCounter,14])

                        #Not need to keep looking
                        TopCounter = TopMessagesArray.shape[0]

                    else:
                        #Not equal, go to next equation
                        MAINtable_array_temp[0,14] = ""
                        TopCounter += 1
                # End while

                if currentRow == 0:
                    MAINtable_array = np.array(MAINtable_array_temp)      
                else:
                    MAINtable_array = np.append(MAINtable_array,MAINtable_array_temp,axis=0)
                #End if Build MAINtable_array

                #Move to next Row on Main page for next flag
                currentRow = currentRow + 1

    TitlesArrayDaily = ["Date", "AC SN", "EICAS Message","MDC Message", "LRU", "ATA", "B1-Equation", "Type",
                        "Equation Description", "Total Occurences", "Consecutive FL",
                        "INTERMITNT", "Reason(s) for flag", "Priority", "Known Top Message - Recommended Documents",
                        "MHIRJ ISE Input", "MEL or No-Dispatch", "MHIRJ ISE Recommendation", "Additional Comments", "Keywords"]

    # Converts the Numpy Array to Dataframe to manipulate
    #pd.set_option('display.max_rows', None)
    # Main table

    OutputTableDaily = pd.DataFrame(data= MAINtable_array, columns= TitlesArrayDaily).fillna(" ").sort_values(by= ["Date", "Type", "Priority"])
    OutputTableDaily = OutputTableDaily.merge(AircraftTailPairDF, on= "AC SN") # Tail # added
    OutputTableDaily = OutputTableDaily[["AC_TN", "Date", "AC SN", "EICAS Message", "MDC Message", "LRU", "ATA", "B1-Equation", "Type",
            "Equation Description", "Total Occurences", "Consecutive FL",
            "INTERMITNT", "Reason(s) for flag", "Priority", "Known Top Message - Recommended Documents",
            "MHIRJ ISE Input", "MEL or No-Dispatch", "MHIRJ ISE Recommendation", "Additional Comments", "Keywords"]] # AC_TN added to output table which means that column order has to be re ordered
    listofJamMessages = ["B1-309178","B1-309179","B1-309180","B1-060044","B1-060045","B1-007973",
                     "B1-060017","B1-006551","B1-240885","B1-006552","B1-006553","B1-006554",
                     "B1-006555","B1-007798","B1-007772","B1-240938","B1-007925","B1-007905",
                     "B1-007927","B1-007915","B1-007926","B1-007910","B1-007928","B1-007920"]
    # all_jam_messages = connect_to_fetch_all_jam_messages()
    # for each_jam_message in all_jam_messages['Jam_Message']:
    #     listofJamMessages.append(each_jam_message)
    # print(listofJamMessages)

    # highlight function starts here
    OutputTableDaily = OutputTableDaily.assign(
        is_jam=lambda dataframe: dataframe['B1-Equation'].map(lambda c: True if c in listofJamMessages else False)
    )
    print(OutputTableDaily)

    OutputTableDaily_json = OutputTableDaily.to_json(orient='records')

    return OutputTableDaily




def dailyReports(occurences, legs, intermittent, consecutiveDays, ata, exclude_EqID, airline_operator, include_current_message, fromDate , toDate):
    MDCdataDF = connect_database_MDCdata(ata, exclude_EqID, airline_operator, include_current_message, fromDate, toDate)
    print("data ", MDCdataDF)
    # Date formatting
    MDCdataDF["MSG_Date"] = pd.to_datetime(MDCdataDF["MSG_Date"])
    # print(MDCdataDF["MSG_Date"])
    MDCdataDF["FLIGHT_LEG"].fillna(value=0.0, inplace=True)  # Null values preprocessing - if 0 = Currentflightphase
    # print(MDCdataDF["FLIGHT_LEG"])
    MDCdataDF["FLIGHT_PHASE"].fillna(False, inplace=True)  # NuCell values preprocessing for currentflightphase
    MDCdataDF["INTERMITNT"].fillna(value=-1, inplace=True)  # Null values preprocessing for currentflightphase
    MDCdataDF["INTERMITNT"].replace(to_replace=">", value=9,
                                      inplace=True)  # > represents greater than 8 INTERMITNT values

    try:                      
        #print("data in intermittent ",MDCdataDF["INTERMITNT"])            
        MDCdataDF["INTERMITNT"] = int(MDCdataDF["INTERMITNT"]) # cast type to int
    except:
        #print("data in intermittent exec",MDCdataDF["INTERMITNT"])            
        MDCdataDF["INTERMITNT"] = 9


    MDCdataDF["AC_SN"] = MDCdataDF["AC_SN"].str.replace('AC', '')
    MDCdataDF.fillna(value=" ", inplace=True)  # replacing all REMAINING null values to a blank string
    MDCdataDF.sort_values(by= "MSG_Date", ascending= False, inplace= True, ignore_index= True)

    AircraftTailPairDF = MDCdataDF[["AC_SN", "AC_TN"]].drop_duplicates(ignore_index= True) # unique pairs of AC SN and AC_TN for use in analysis
    AircraftTailPairDF.columns = ["AC SN","AC_TN"] # re naming the columns to match History/Daily analysis output

    DatesinData = MDCdataDF["MSG_Date"].dt.date.unique()  # these are the dates in the data in Datetime format.
    NumberofDays = len(MDCdataDF["MSG_Date"].dt.date.unique())  # to pass into Daily analysis number of days in data
    latestDay = str(MDCdataDF.loc[0, "MSG_Date"].date())  # to pass into Daily analysis
    LastLeg = max(MDCdataDF["FLIGHT_LEG"])  # Latest Leg in the data
    MDCdataArray = MDCdataDF.to_numpy()  # converting to numpy to work with arrays

    # MDCMessagesDF = pd.read_csv(MDCMessagesURL_path, encoding="utf8")  # bring messages and inputs into a Dataframe
    MDCMessagesDF = connect_database_MDCmessagesInputs()
    MDCMessagesArray = MDCMessagesDF.to_numpy()  # converting to numpy to work with arrays
    ShapeMDCMessagesArray = MDCMessagesArray.shape  # tuple of the shape of the MDC message data (#rows, #columns)

    # TopMessagesDF = pd.read_csv(TopMessagesURL_path)  # bring messages and inputs into a Dataframe
    TopMessagesDF = connect_database_TopMessagesSheet()
    TopMessagesArray = TopMessagesDF.to_numpy()  # converting to numpy to work with arrays


    UniqueSerialNumArray = []

    if(airline_operator.upper() == "SKW"):
        airline_id = 101
    Flagsreport = 1  # this is here to initialize the variable. user must start report by choosing Newreport = True
    # Flagsreport: int, AircraftSN: str, , newreport: int, CurrentFlightPhaseEnabled: int
    if(include_current_message == 1):
        CurrentFlightPhaseEnabled = 1
    else:
        CurrentFlightPhaseEnabled = 0  # 1 or 0, 1 includes current phase, 0 does not include current phase

    MaxAllowedOccurances = occurences  # flag for Total number of occurences -> T
    MaxAllowedConsecLegs = legs  # flag for consecutive legs -> CF
    MaxAllowedConsecDays = consecutiveDays
    if intermittent > 9:
        MaxAllowedIntermittent = 9  # flag for intermittent values ->IM
    else:
        MaxAllowedIntermittent = intermittent  # flag for intermittent values ->IM
    #Bcode = equationID
    newreport = True    #set a counter variable to bring back it to false

    #global UniqueSerialNumArray

    # function to separate the chunks of data and convert it into a numpy array
    def separate_data(data, date):
        '''Takes data as a dataframe, along with a date to slice the larger data to only include the data in that date'''

        DailyDataDF = data.loc[date]
        return DailyDataDF

    AnalysisDF = MDCdataDF.set_index(
        "MSG_Date")  # since MSG_Date was moved to the index of the DF, the column values change from the original MDCdataDF

    currentRow = 0
    MAINtable_array_temp = np.empty((1, 17), object)  # 18 for the date #input from user
    MAINtable_array = []

    # will loop through each day to slice the data for each day, then initialize arrays to individually analyze each day

    for i in range(0, NumberofDays):
        daytopass = str(DatesinData[i])

        # define array to analyze
        DailyanalysisDF = separate_data(AnalysisDF, daytopass)

        ShapeDailyanalysisDF = DailyanalysisDF.shape  # tuple of the shape of the daily data (#rows, #columns)
        DailyanalysisArray = DailyanalysisDF.to_numpy()  # slicing the array to only include the daily data
        NumAC = DailyanalysisDF["AC_SN"].nunique()  # number of unique aircraft SN in the data
        UniqueSerialNumArray = DailyanalysisDF.AC_SN.unique()  # unique aircraft values
        SerialNumFreqSeries = DailyanalysisDF.AC_SN.value_counts()  # the index of this var contains the AC with the most occurrences
        MaxOfAnAC = SerialNumFreqSeries[0]  # the freq series sorts in descending order, max value is top

        # Define the arrays as numpy
        MDCeqns_array = np.empty((MaxOfAnAC, NumAC), object)  # MDC messages for each AC stored in one array
        MDCLegs_array = np.empty((MaxOfAnAC, NumAC),
                                    object)  # Flight Legs for a message for each AC stored in one array
        MDCIntermittent_array = np.empty((MaxOfAnAC, NumAC),
                                            object)  # stores the intermittence values for each message of each array
        FourDigATA_array = np.empty((MaxOfAnAC, NumAC), object)  # stores the ATAs of each message in one array

        if CurrentFlightPhaseEnabled == 1:  # Show all, current and history
            MDCFlightPhase_array = np.ones((MaxOfAnAC, NumAC), int)
        elif CurrentFlightPhaseEnabled == 0:  # Only show history
            MDCFlightPhase_array = np.empty((MaxOfAnAC, NumAC), object)

        Messages_LastRow = ShapeMDCMessagesArray[0]  # taken from the shape of the array (3519 MDC messages total)
        Flags_array = np.empty((Messages_LastRow, NumAC), object)
        FlightLegsEx = 'Flight legs above 32,600 for the following A/C: '  # at 32767 the DCU does not incrementmore the flight counter, so the MDC gets data for the same 32767 over and over until the limit of MDC logs per flight leg is reached (20 msgs per leg), when reached the MDC stops storing data since it gets always the same 32767
        TotalOccurances_array = np.empty((Messages_LastRow, NumAC), int)
        ConsecutiveLegs_array = np.empty((Messages_LastRow, NumAC), int)
        IntermittentInLeg_array = np.empty((Messages_LastRow, NumAC), int)

        # 2D array looping, columns (SNcounter) rows (MDCsheetcounter)
        for SNCounter in range(0, NumAC):  # start counter over each aircraft (columns)

            MDCArrayCounter = 0  # rows of each different array

            for MDCsheetCounter in range(0, ShapeDailyanalysisDF[0]):  # counter over each entry  (rows)

                # If The Serial number on the dailyanalysisarray matches the current Serial Number, copy
                if DailyanalysisArray[MDCsheetCounter, 0] == UniqueSerialNumArray[SNCounter]:
                    # Serial numbers match, record information
                    #       SNcounter -->
                    # format for these arrays :   | AC1 | AC2 | AC3 |.... | NumAC
                    # MDCarraycounter(vertically)| xx | xx | xx |...
                    MDCeqns_array[MDCArrayCounter, SNCounter] = DailyanalysisArray[
                        MDCsheetCounter, 13]  # since MSG_Date is the index, 13 corresponds to the equations column, where in the history analysis is 14
                    MDCLegs_array[MDCArrayCounter, SNCounter] = DailyanalysisArray[MDCsheetCounter, 2]
                    MDCIntermittent_array[MDCArrayCounter, SNCounter] = DailyanalysisArray[
                        MDCsheetCounter, 12]  # same as above ^
                    FourDigATA_array[MDCArrayCounter, SNCounter] = DailyanalysisArray[MDCsheetCounter, 5]

                    if DailyanalysisArray[MDCsheetCounter, 10]:
                        FourDigATA_array[MDCArrayCounter, SNCounter] = DailyanalysisArray[MDCsheetCounter, 5]

                    if CurrentFlightPhaseEnabled == 0:  # populating the empty array
                        MDCFlightPhase_array[MDCArrayCounter, SNCounter] = DailyanalysisArray[MDCsheetCounter, 10]

                    MDCArrayCounter = MDCArrayCounter + 1

            # arrays with the same size as the MDC messages sheet (3519) checks if each message exists in each ac
            for MessagessheetCounter in range(0, Messages_LastRow):

                # Initialize Counts, etc

                # Total Occurances
                eqnCount = 0

                # Consecutive Legs
                ConsecutiveLegs = 0
                MaxConsecutiveLegs = 0
                tempLeg = LastLeg

                # INTERMITNT
                IntermittentFlightLegs = 0

                MDCArrayCounter = 0

                while MDCArrayCounter < MaxOfAnAC:
                    if MDCeqns_array[MDCArrayCounter, SNCounter]:
                        # Not Empty, and not current                                      B code
                        if MDCeqns_array[MDCArrayCounter, SNCounter] == MDCMessagesArray[MessagessheetCounter, 12] \
                                and MDCFlightPhase_array[MDCArrayCounter, SNCounter]:

                            # Total Occurances
                            # Count this as 1 occurance
                            eqnCount = eqnCount + 1

                            # Consecutive Days not used in the daily analysis DO FOR HISTORY

                            # Consecutive Legs
                            if MDCLegs_array[MDCArrayCounter, SNCounter] == tempLeg:

                                tempLeg = tempLeg - 1
                                ConsecutiveLegs = ConsecutiveLegs + 1

                                if ConsecutiveLegs > MaxConsecutiveLegs:
                                    MaxConsecutiveLegs = ConsecutiveLegs

                            else:

                                # If not consecutive, start over
                                ConsecutiveLegs = 1
                                tempLeg = MDCLegs_array[MDCArrayCounter, SNCounter]

                            # INTERMITNT
                            # Taking the maximum intermittent value - come back to this and implement max function for an column
                            x = MDCIntermittent_array[MDCArrayCounter, SNCounter]
                            if isinstance(x, numbers.Number) and MDCIntermittent_array[
                                MDCArrayCounter, SNCounter] > IntermittentFlightLegs:
                                IntermittentFlightLegs = MDCIntermittent_array[MDCArrayCounter, SNCounter]
                            # End if INTERMITNT numeric check

                            # Other
                            # Check that the legs is not over the given limit
                            Flags_array[MessagessheetCounter, SNCounter] = ''
                            if MDCLegs_array[MDCArrayCounter, SNCounter] > 32600:
                                FlightLegsEx = FlightLegsEx + str(UniqueSerialNumArray[SNCounter]) + ' (' + str(
                                    MDCLegs_array[MDCArrayCounter, SNCounter]) + ')' + ' '
                            # End if Legs flag

                            # Check for Other Flags
                            if MDCMessagesArray[MessagessheetCounter, 13]:
                                # Immediate (occurrance flag in excel MDC Messages sheet)
                                if MDCMessagesArray[MessagessheetCounter, 13] == 1:
                                    # Immediate Flag required
                                    Flags_array[MessagessheetCounter, SNCounter] = str(
                                        MDCMessagesArray[MessagessheetCounter, 12]) + " occured at least once."
                        MDCArrayCounter += 1

                    else:
                        MDCArrayCounter = MaxOfAnAC

                        # Next MDCArray Counter

                TotalOccurances_array[MessagessheetCounter, SNCounter] = eqnCount
                ConsecutiveLegs_array[MessagessheetCounter, SNCounter] = MaxConsecutiveLegs
                IntermittentInLeg_array[MessagessheetCounter, SNCounter] = IntermittentFlightLegs

            # Next MessagessheetCounter
        # Next SNCounter

        for SNCounter in range(0, NumAC):

            for EqnCounter in range(0, Messages_LastRow):

                # Continue with Report
                if TotalOccurances_array[EqnCounter, SNCounter] >= MaxAllowedOccurances \
                        or ConsecutiveLegs_array[EqnCounter, SNCounter] >= MaxAllowedConsecLegs \
                        or IntermittentInLeg_array[EqnCounter, SNCounter] >= MaxAllowedIntermittent \
                        or Flags_array[EqnCounter, SNCounter]:

                    # Populate Flags Array
                    if TotalOccurances_array[EqnCounter, SNCounter] >= MaxAllowedOccurances:
                        Flags_array[EqnCounter, SNCounter] = Flags_array[
                                                                    EqnCounter, SNCounter] + "Total occurances exceeded " + str(
                            MaxAllowedOccurances) + " occurances. "

                    if ConsecutiveLegs_array[EqnCounter, SNCounter] >= MaxAllowedConsecLegs:
                        Flags_array[EqnCounter, SNCounter] = Flags_array[
                                                                    EqnCounter, SNCounter] + "Maximum consecutive flight legs exceeded " + str(
                            MaxAllowedConsecLegs) + " flight legs. "

                    if IntermittentInLeg_array[EqnCounter, SNCounter] >= MaxAllowedIntermittent:
                        Flags_array[EqnCounter, SNCounter] = Flags_array[
                                                                    EqnCounter, SNCounter] + "Maximum intermittent occurances for one flight leg exceeded " + str(
                            MaxAllowedIntermittent) + " occurances. "

                    # populating the final array (Table)
                    MAINtable_array_temp[0, 0] = daytopass
                    MAINtable_array_temp[0, 1] = UniqueSerialNumArray[SNCounter]
                    MAINtable_array_temp[0, 2] = MDCMessagesArray[EqnCounter, 8]
                    MAINtable_array_temp[0, 3] = MDCMessagesArray[EqnCounter, 0]
                    MAINtable_array_temp[0, 4] = MDCMessagesArray[EqnCounter, 1]
                    MAINtable_array_temp[0, 5] = MDCMessagesArray[EqnCounter, 12]
                    MAINtable_array_temp[0, 6] = MDCMessagesArray[EqnCounter, 7]
                    MAINtable_array_temp[0, 7] = MDCMessagesArray[EqnCounter, 11]
                    MAINtable_array_temp[0, 8] = TotalOccurances_array[EqnCounter, SNCounter]

                    MAINtable_array_temp[0, 9] = ConsecutiveLegs_array[EqnCounter, SNCounter]
                    MAINtable_array_temp[0, 10] = IntermittentInLeg_array[EqnCounter, SNCounter]
                    MAINtable_array_temp[0, 11] = Flags_array[EqnCounter, SNCounter]

                    # if the input is empty set the priority to 4
                    if MDCMessagesArray[EqnCounter, 15] == 0:
                        MAINtable_array_temp[0, 12] = 4
                    else:
                        MAINtable_array_temp[0, 12] = MDCMessagesArray[EqnCounter, 15]

                    # For B1-006424 & B1-006430 Could MDC Trend tool assign Priority 3 if logged on A/C below 10340, 15317. Priority 1 if logged on 10340, 15317, 19001 and up
                    if MDCMessagesArray[EqnCounter, 12] == "B1-006424" or MDCMessagesArray[
                        EqnCounter, 12] == "B1-006430":
                        if int(UniqueSerialNumArray[SNCounter]) <= 10340 and int(
                                UniqueSerialNumArray[SNCounter]) > 10000:
                            MAINtable_array_temp[0, 12] = 3
                        elif int(UniqueSerialNumArray[SNCounter]) > 10340 and int(
                                UniqueSerialNumArray[SNCounter]) < 11000:
                            MAINtable_array_temp[0, 12] = 1
                        elif int(UniqueSerialNumArray[SNCounter]) <= 15317 and int(
                                UniqueSerialNumArray[SNCounter]) > 15000:
                            MAINtable_array_temp[0, 12] = 3
                        elif int(UniqueSerialNumArray[SNCounter]) > 15317 and int(
                                UniqueSerialNumArray[SNCounter]) < 16000:
                            MAINtable_array_temp[0, 12] = 1
                        elif int(UniqueSerialNumArray[SNCounter]) >= 19001 and int(
                                UniqueSerialNumArray[SNCounter]) < 20000:
                            MAINtable_array_temp[0, 12] = 1

                            # check the content of MHIRJ ISE recommendation and add to array
                    if MDCMessagesArray[EqnCounter, 16] == 0:
                        MAINtable_array_temp[0, 14] = ""
                    else:
                        MAINtable_array_temp[0, 14] = MDCMessagesArray[EqnCounter, 16]

                    # check content of "additional"
                    if MDCMessagesArray[EqnCounter, 17] == 0:
                        MAINtable_array_temp[0, 15] = ""
                    else:
                        MAINtable_array_temp[0, 15] = MDCMessagesArray[EqnCounter, 17]

                    # check content of "MHIRJ Input"
                    if MDCMessagesArray[EqnCounter, 18] == 0:
                        MAINtable_array_temp[0, 16] = ""
                    else:
                        MAINtable_array_temp[0, 16] = MDCMessagesArray[EqnCounter, 18]

                    # Check for the equation in the Top Messages sheet
                    TopCounter = 0
                    Top_LastRow = TopMessagesArray.shape[0]
                    while TopCounter < Top_LastRow:

                        # Look for the flagged equation in the Top Messages Sheet
                        if MDCMessagesArray[EqnCounter][12] == TopMessagesArray[TopCounter, 4]:

                            # Found the equation in the Top Messages Sheet. Put the information in the last column
                            MAINtable_array_temp[0, 13] = "Known Nuissance: " + str(TopMessagesArray[TopCounter, 13]) \
                                                            + " / In-Service Document: " + str(
                                TopMessagesArray[TopCounter, 11]) \
                                                            + " / FIM Task: " + str(TopMessagesArray[TopCounter, 10]) \
                                                            + " / Remarks: " + str(TopMessagesArray[TopCounter, 14])

                            # Not need to keep looking
                            TopCounter = TopMessagesArray.shape[0]

                        else:
                            # Not equal, go to next equation
                            MAINtable_array_temp[0, 13] = ""
                            TopCounter += 1
                    # End while

                    if currentRow == 0:
                        MAINtable_array = np.array(MAINtable_array_temp)
                    else:
                        MAINtable_array = np.append(MAINtable_array, MAINtable_array_temp, axis=0)
                    # End if Build MAINtable_array

                    # Move to next Row on Main page for next flag
                    currentRow = currentRow + 1

    TitlesArrayDaily = ["Date", "AC SN", "EICAS Message", "LRU", "ATA", "B1-Equation", "Type",
                        "Equation Description", "Total Occurences", "Consecutive FL",
                        "INTERMITNT", "Reason(s) for flag", "Priority", "Known Top Message - Recommended Documents",
                        "MHIRJ ISE Recommendation", "Additional Comments", "MHIRJ ISE Input"]
    # Converts the Numpy Array to Dataframe to manipulate
    # pd.set_option('display.max_rows', None)
    OutputTableDaily = pd.DataFrame(data=MAINtable_array, columns=TitlesArrayDaily).fillna(" ").sort_values(
        by=["Date", "Type", "Priority"])
    OutputTableDaily = OutputTableDaily.merge(AircraftTailPairDF, on= "AC SN") # Tail # added
    OutputTableDaily = OutputTableDaily[["AC_TN", "Date", "AC SN", "EICAS Message", "LRU", "ATA", "B1-Equation", "Type",
            "Equation Description", "Total Occurences", "Consecutive FL",
            "INTERMITNT", "Reason(s) for flag", "Priority", "Known Top Message - Recommended Documents",
            "MHIRJ ISE Recommendation", "Additional Comments", "MHIRJ ISE Input"]] # AC_TN added to output table which means that column order has to be re ordered
    

    listofJamMessages = list()
    # all_jam_messages = connect_to_fetch_all_jam_messages()
    # for each_jam_message in all_jam_messages['Jam_Message']:
    #     listofJamMessages.append(each_jam_message)
    # print(listofJamMessages)

    # highlight function starts here
    OutputTableDaily = OutputTableDaily.assign(
        is_jam=lambda dataframe: dataframe['B1-Equation'].map(lambda c: True if c in listofJamMessages else False)
    )
    print(OutputTableDaily)

    OutputTableDaily_json = OutputTableDaily.to_json(orient='records')

    # return OutputTableDaily_json
    return OutputTableDaily