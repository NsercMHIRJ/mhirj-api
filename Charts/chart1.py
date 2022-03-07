from Charts.helper import connect_database_for_chart1_test,connect_database_for_chart1_test2
import pandas as pd
import json
import numpy as np

def chart_one(top_n, aircraftNo,ata_main, fromDate,toDate):
# @app.post("/api/chart_one/{top_n}/{aircraftNo}/{fromDate}/{toDate}")
# async def get_ChartOneData(top_n:int, aircraftNo:int,  fromDate: str , toDate: str):
    # chart1_sql_df = connect_database_for_chart1(top_n, aircraftNo, ata_main, fromDate, toDate)
    # chart1_sql_df_json = chart1_sql_df.to_json(orient='records')
    # return chart1_sql_df_json
    AircraftToStudy=aircraftNo
    Topvalues=top_n
    MDCdataDF = connect_database_for_chart1_test(fromDate, toDate)
    MDCMessagesDF=connect_database_for_chart1_test2()

    # include the current flight legs
    selection = MDCdataDF[["EQ_ID", "AC_SN"]].copy()
    print("---selection---")
    print(selection)
    total_occ_DF = selection.value_counts().unstack()

    print("--------total-----")
    print(total_occ_DF)
    print( total_occ_DF[AircraftToStudy])
    print("--done--")
    
    # B1 Messages Occurrence per Aircraft    
    messagestoshow = total_occ_DF[AircraftToStudy].sort_values().dropna().tail(Topvalues).index.to_frame(index= False, name = "Message")
    print("--------message to show----")
    print(messagestoshow)
    messagesdescriptionDF = MDCMessagesDF[["Equation_ID", "Message", "EICAS", "LRU", "ATA"]].set_index(["Equation_ID"])
    print("-------messagesdescriptionDF-----")
    print(messagesdescriptionDF)

    def Definelabels(messages, Messagedata= messagesdescriptionDF):
        '''Input of dataframe containing Bcode messages to be matched and joined with their respective descriptions'''
        for i in range(len(messages)):
            Bcode = messages.at[i, "Message"]
            LRUdata = Messagedata.loc[Bcode, "LRU"]
            ATAdata = Messagedata.loc[Bcode, "ATA"]
            messages.at[i, "Message"] = str(messages.at[i, "Message"] + "\n" + LRUdata + "\n" + ATAdata)
        return messages["Message"].to_list()

    Plottinglabels = Definelabels(messagestoshow)
    print("-----plotting lables-----")
    print(Plottinglabels)
    print(type(Plottinglabels))
    Plottingarray = total_occ_DF[AircraftToStudy].sort_values().dropna().tail(Topvalues).to_list()
    print("---plotting array-----")
    print(Plottingarray)
    print(type(Plottingarray))

    final= list(zip(Plottinglabels,Plottingarray))
    print("----final----")
    print(final)

    final_df= pd.DataFrame (final,columns = ['LRU','total_message'])

    chart1_sql_df_json = final_df.to_json(orient='records')
    return chart1_sql_df_json

    