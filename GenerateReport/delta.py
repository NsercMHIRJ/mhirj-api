from GenerateReport.history import historyReport
import pandas as pd
import json

def create_delta_lists(prev_history, curr_history):
    '''create True and False lists for the Delta report'''
    # making tuples of the combos of AC SN and B1-eqn
    comb_prev = list(zip(prev_history["AC SN"], prev_history["B1-Equation"]))
    comb_curr = list(zip(curr_history["AC SN"], curr_history["B1-Equation"]))
 
    # create a list for flags still on going (true_list) and new flags (false_list)
    True_list = []
    False_list = []
    for i in range(len(comb_curr)):
        if comb_curr[i] in comb_prev:
            True_list.append(comb_curr[i])
        elif comb_curr[i] not in comb_prev:
            False_list.append(comb_curr[i])
 
       
        
    return True_list, False_list

def deltaReport(occurences, legs, intermittent, consecutiveDays, ata, exclude_EqID, airline_operator, include_current_message, prev_fromDate, prev_toDate, curr_fromDate, curr_toDate):
    listofJamMessages = ["B1-309178","B1-309179","B1-309180","B1-060044","B1-060045","B1-007973",
                        "B1-060017","B1-006551","B1-240885","B1-006552","B1-006553","B1-006554",
                        "B1-006555","B1-007798","B1-007772","B1-240938","B1-007925","B1-007905",
                        "B1-007927","B1-007915","B1-007926","B1-007910","B1-007928","B1-007920"]
    curr_history_dataframe = historyReport(occurences, legs, intermittent, consecutiveDays, ata, exclude_EqID, airline_operator, include_current_message, curr_fromDate, curr_toDate)
    prev_history_dataframe = historyReport(occurences, legs, intermittent, consecutiveDays, ata, exclude_EqID, airline_operator, include_current_message, prev_fromDate, prev_toDate)
    # curr_history_dataframe = pd.read_json(curr_history_json)
    # prev_history_dataframe = pd.read_json(prev_history_json)
    True_list, False_list = create_delta_lists(prev_history_dataframe, curr_history_dataframe)

    curr_history_dataframe.set_index(["AC SN", "B1-Equation"], drop= False, inplace= True)
    prev_history_dataframe.set_index(["AC SN", "B1-Equation"], drop= False, inplace= True)
    # grab only what exists in the new report from the old report
    prev_history_nums = prev_history_dataframe.loc[True_list]
    # add the items that only exist in the new report and add them to the old report 
    # (this has to be after the True/False Lists are created, bc if done before then everything will be True (list))
    # this was done bc the slicing done for slice_ needs to compare two dataframes with identical indexes
    prev_history_nums = prev_history_nums.append(curr_history_dataframe.loc[False_list])
    # sort indexes
    curr_history_dataframe.sort_index(inplace= True)
    prev_history_nums.sort_index(inplace= True)
    idx = pd.IndexSlice
    # comparing the counters on each report 
    # since there are some rows that are added to the prev_history_nums (see above), 
    # the values on both dataframes will be equal at the corresponding indexes and wont be highlighted 
    # the logic here will only highlight whats strictly greater
    slice_ = idx[idx[curr_history_dataframe["Total Occurrences"] > prev_history_nums["Total Occurrences"]], ["Total Occurrences"]]
    slice_2 = idx[idx[curr_history_dataframe["Consecutive Days"] > prev_history_nums["Consecutive Days"]], ["Consecutive Days"]]
    slice_3 = idx[idx[curr_history_dataframe["Consecutive FL"] > prev_history_nums["Consecutive FL"]], ["Consecutive FL"]]
    slice_4 = idx[idx[curr_history_dataframe["INTERMITNT"] > prev_history_nums["INTERMITNT"]], ["INTERMITNT"]]
    # delta = delta.set_properties(**{'background-color': '#fabf8f'}, subset=slice_)
    # delta = delta.set_properties(**{'background-color': '#fabf8f'}, subset=slice_2)
    # delta = delta.set_properties(**{'background-color': '#fabf8f'}, subset=slice_3)
    # delta = delta.set_properties(**{'background-color': '#fabf8f'}, subset=slice_4)
    # delta.to_excel(input("Input the desired filename, '.xlsx' is added automatically: ") + ".xlsx", index= False)
    i = 0
    delta = list()
    for item in prev_history_nums.values:
        tmpData = {'Tail#': item[0], 'AC SN': item[1], 'EICAS Related': item[2], 'MDC Message': item[3],'LRU': item[4], 'ATA': item[5],
            'B1-Equation': item[6], 'Type': item[7], 'Equation Description': item[8], 'Total Occurrences': item[9] , 'Consecutive Days': item[10],
            'Consecutive FL': item[11], 'INTERMITNT': item[12], 'Date From': str(item[13]), 'Date To': str(item[14]), 'Reason(s) for flag': item[15],
            'Priority': item[16], 'MHIRJ Known Message': item[17], 'Mel or No-Dispatch': item[18], 'MHIRJ Input': item[19], 'MHIRJ Recommended Action': item[20], 
            'MHIRJ Additional Comment': item[21], 'Jam': item[22]}
        if i < curr_history_dataframe.loc[False_list].values.size:
            if item[0] in curr_history_dataframe.loc[False_list].values:
                if item[5] in listofJamMessages:
                    tmpData['backgroundcolor'] = "#ff342e"
                else:
                    tmpData['backgroundcolor'] = "#fabf8f"
        if i < prev_history_dataframe.loc[True_list].values.size:
            if item[0] in prev_history_dataframe.loc[True_list].values:     
                if item[5] in listofJamMessages:
                    tmpData['backgroundcolor'] = "#f08080"
                else:
                    tmpData['backgroundcolor'] = "#fde9d9"
                    if slice_[0].values[i]:
                        tmpData['Total Occurrences Col'] =  "#fabf8f"
                    if slice_2[0].values[i]:
                        tmpData['Consecutive Days Col'] = "#fabf8f"
                    if slice_3[0].values[i]:
                        tmpData['Consecutive FL Col'] = "#fabf8f"
                    if slice_4[0].values[i]:
                        tmpData['INTERMITNT Col'] = "#fabf8f"
        
        delta.append(tmpData)
        i += 1
    return delta