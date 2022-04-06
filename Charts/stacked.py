from Charts.helper import connect_db_MDCdata_chartb_ata
import pandas as pd
import json
import numpy as np

def stacked_chart(ata, top_n, from_dt,to_dt):

# @app.post("/api/Landing_Chart_B/{ata}/{top_n}/{from_dt}/{to_dt}")
# def get_Chart_B(ata:str,top_n: int,from_dt: str, to_dt: str):
    try:
        Topvalues2 = top_n
        if Topvalues2>50:
            Topvalues2=50
        
        MDCdataDF = connect_db_MDCdata_chartb_ata(ata,from_dt, to_dt)
        AircraftTailPairDF = MDCdataDF[["AC_SN", "AC_TN"]].drop_duplicates(ignore_index= True) # unique pairs of AC SN and Tail# for use in analysis
        print("---------------test--------------")
        print(AircraftTailPairDF)
        AircraftTailPairDF.columns = ["AC SN","Tail"] # re naming the columns to match History/Daily analysis output
        print("---------------test2--------------")
        print(AircraftTailPairDF.columns)
        chartADF = pd.merge(left = MDCdataDF[["AC_SN","ATA_Main", "EQ_ID"]], right = AircraftTailPairDF, left_on="AC_SN", right_on="AC SN")
        print("---------------test3--------------")
        print(chartADF)
        chartADF["AC_SN"] = chartADF["AC_SN"] + " / " + chartADF["Tail"]
        print("---------------test4--------------")
        print(chartADF["AC_SN"])
        chartADF.drop(labels = ["AC SN", "Tail"], axis = 1, inplace = True)
        MessageCountbyAircraftATA = chartADF.groupby(["AC_SN","ATA_Main"]).count()
        print("----test5-----------")
        print(MessageCountbyAircraftATA)
        # https://towardsdatascience.com/stacked-bar-charts-with-pythons-matplotlib-f4020e4eb4a7
        # https://stackoverflow.com/questions/44309507/stacked-bar-plot-using-matplotlib
        # transpose the indexes. where the ATA label becomes the column and the aircraft is row. counts are middle
        TransposedMessageCountbyAircraftATA = MessageCountbyAircraftATA["EQ_ID"].unstack()
        # fill Null values with 0
        TransposedMessageCountbyAircraftATA.fillna(value= 0, inplace= True)

        # sum all the counts by row, plus create a new column called sum
        TransposedMessageCountbyAircraftATA["Sum"] = TransposedMessageCountbyAircraftATA.sum(axis=1)

        # sort the dataframe by the values of sum, and from the topvalues2 the user chooses
        TransposedMessageCountbyAircraftATA = TransposedMessageCountbyAircraftATA.sort_values("Sum").tail(Topvalues2)
        TransposedMessageCountbyAircraftATA = TransposedMessageCountbyAircraftATA.sort_values("Sum", ascending=False)

        # create a final dataframe for plotting without the new column created before
        TransposedMessageCountbyAircraftATAfinalPLOT = TransposedMessageCountbyAircraftATA.drop(["Sum"], axis=1)
        print('TransposedMessageCountbyAircraftATAfinalPLOT colums : ',TransposedMessageCountbyAircraftATAfinalPLOT.columns)
        #totals = TransposedMessageCountbyAircraftATA["Sum"]
        print("total in landing chart B is : ",TransposedMessageCountbyAircraftATAfinalPLOT)
        # TransposedMessageCountbyAircraftATAfinalPLOT = TransposedMessageCountbyAircraftATAfinalPLOT.sort_values(by='ATA Main',ascending=False)
        chart_b_df_json = TransposedMessageCountbyAircraftATAfinalPLOT.to_json(orient='index')
	    #return chart_b_df_json
        # #image settings
        # ax8 = TransposedMessageCountbyAircraftATAfinalPLOT.plot(kind='barh', stacked=True, figsize=(16, 9))
        # ax8.set_ylabel('Aircraft Serial Number')
        # ax8.set_title('Magnitude of messages in data')
        # ax8.grid(b= True, which= "both", axis= "x", alpha= 0.3)
        # rects8 = ax8.containers[-1] 


        # # here to add column labeling
        # for i, total in enumerate(totals):
        #     ax8.text(totals[i], rects8[i].get_y() +0.15 , round(total), ha='left')
            
        # plt.show()
        return chart_b_df_json
    except Exception as es :
 	    print(es)
