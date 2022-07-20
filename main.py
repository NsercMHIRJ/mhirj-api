# Importing libraries to the project
#!/usr/bin/bash
from Charts.chart4 import chart4Report
from Charts.chart5 import chart5Report
from Charts.chart2 import chart_two
from Charts.chart1 import chart_one
from Charts.stacked import stacked_chart
from Charts.Landing_chartB import Landing_chartB


from GenerateReport.daily import dailyReport
from GenerateReport.filters import db_insert_filter_data
from GenerateReport.history import historyReport
from Charts.chart3 import chart3Report
import numpy as np
import pandas as pd
import sys
import pyodbc
import json
from GenerateReport.delta import deltaReport
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional
import os
import urllib
from fastapi import File, UploadFile
from crud import *
import uvicorn
from util.util import connect_database_for_corelation_ata, connect_database_for_corelation_pid, connect_database_for_corelation_tail, connect_database_mdc_message_input , connect_database_MDCdata , connect_to_fetch_all_ata , connect_to_fetch_all_eqids, db_delete_mdc_messages_input_by_eq_id, db_insert_mdc_messages_input
from GenerateReport.jamReport import jamReport
from GenerateReport.jamReport import mdcDF
from GenerateReport.flagReport import Toreport


app = FastAPI()

#Cors
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

if __name__ == "__main__":
    uvicorn.run("main:app",host="0.0.0.0",port=8000,log_level="info")

# Initialiaze Database Properties
hostname = os.environ.get('hostname', 'aftermarket-mhirj.database.windows.net')
db_server_port = urllib.parse.quote_plus(str(os.environ.get('db_server_port', '8000')))
db_name = os.environ.get('db_name', 'MDP-Dev')
db_username = urllib.parse.quote_plus(str(os.environ.get('db_username', 'humber_ro')))
db_password = urllib.parse.quote_plus(str(os.environ.get('db_password', 'Container-Zesty-Wriggly7-Catalog')))
ssl_mode = urllib.parse.quote_plus(str(os.environ.get('ssl_mode','prefer')))
db_driver = "ODBC Driver 17 for SQL Server"


MDCdataDF = pd.DataFrame()


def connect_database_for_chartb(from_dt, to_dt,aircraftno):
    sql = "SELECT AC_SN,EQ_ID,SUBSTRING(ATA, 0, CHARINDEX('-', ATA)) AS ATA_Main FROM MDC_MSGS WHERE AC_SN= "+str(aircraftno)+" AND MSG_Date BETWEEN '" + from_dt + "' AND '" + to_dt + "'" 
    column_names = ["AC_MODEL", "AC_SN", "AC_TN",
                    "OPERATOR", "MSG_TYPE", "MDC_SOFTWARE", "MDT_VERSION", "MSG_Date",
                    "FLIGHT_NUM","FLIGHT_LEG", "FLIGHT_PHASE", "ATA", "ATA_NAME", "LRU",
                    "COMP_ID", "MSG_TXT","EQ_ID", "INTERMITNT", "EVENT_NOTE",
                    "EQ_TS_NOTE","SOURCE", "MSG_ID", "FALSE_MSG","BOOKMARK","msg_status"]
    try:
        conn = pyodbc.connect(driver=db_driver, host=hostname, database=db_name,
                              user=db_username, password=db_password)
        MDCdataDF_chartb = pd.read_sql(sql, conn)
        conn.close()
        return MDCdataDF_chartb
    except pyodbc.Error as err:
        print("Couldn't connect to Server")
        print("Error message:- " + str(err))

#chartb(new chart)-----------------------------------------

@app.post("/api/chart_b/{from_date}/{to_date}/{aircraftno}")
def get_ScatterChart_MDC_PM_Data(from_date:str, to_date:str, aircraftno:int):
    MDCdataDF = connect_database_for_chartb(from_date, to_date, aircraftno)
    Aircrafttostudy5=str(aircraftno)

    # groups the data by Aircraft and Main ATA, produces a count of values in each ata by counting entries in Equation ID
    MessageCountbyAircraftATA2 = MDCdataDF[["AC_SN","ATA_Main", "EQ_ID"]].groupby(["AC_SN","ATA_Main"]).count()

    # transpose the indexes. where the ATA label becomes the column and the aircraft is row. counts are middle
    TransposedMessageCountbyAircraftATA2 = MessageCountbyAircraftATA2["EQ_ID"].unstack()
    print("---------------transpose-------------")
    print(TransposedMessageCountbyAircraftATA2)
    
    Count = TransposedMessageCountbyAircraftATA2.to_json(orient='records')
    return Count        


#-----------Raw data------------------
@app.get("/api/RawData/{fromDate}/{toDate}/")
def get_MDCRawData(fromDate,toDate, ata : Optional[str] = '', eqID : Optional[str] = '', msg : Optional[str] = ''):
    message = 0
    if msg:
        message = int(msg)
    c = connect_database_MDCdata(ata, eqID, message, fromDate, toDate)
    print(c['MSG_Date'])
    print(type(c['MSG_Date']))
    MDCdataDF_json = c.to_json(orient='records')
    return MDCdataDF_json




#--------------Report(history/Daily)------------------
@app.post("/api/GenerateReport/{analysisType}/{occurences}/{legs}/{intermittent}/{consecutiveDays}/{ata}/{exclude_EqID}/{airline_operator}/{include_current_message}/{aircraft_no}/{fromDate}/{toDate}")
def generateReport(analysisType: str, occurences: int, legs: int, intermittent: int, consecutiveDays: int, ata: str, exclude_EqID:str, airline_operator: str, include_current_message: int, aircraft_no: str, fromDate: str , toDate: str):
    print(fromDate, " ", toDate)
    if analysisType.lower() == "history":
        respObj = historyReport(occurences, legs, intermittent, consecutiveDays, ata, exclude_EqID, airline_operator, include_current_message, aircraft_no, fromDate , toDate)
        return respObj.to_json(orient='records')
    
    respObj = dailyReport(occurences, legs, intermittent, consecutiveDays, ata, exclude_EqID, airline_operator, include_current_message, aircraft_no, fromDate , toDate).to_json(orient='records')
    return respObj


#--------jamReport-----------------
@app.post(
   "/api/GenerateReport/{analysisType}/{occurences}/{legs}/{intermittent}/{consecutiveDays}/{ata}/{exclude_EqID}/{airline_operator}/{include_current_message}/{aircraft_no}/{fromDate}/{toDate}/{ACSN_chosen}")
def generateJamsReport(analysisType: str, occurences: int, legs: int, intermittent: int, consecutiveDays: int,
                        ata: str, exclude_EqID: str, airline_operator: str, include_current_message: int, aircraft_no: str,
                        fromDate: str, toDate: str, ACSN_chosen:int):
   
   if (analysisType.lower() == "history"):
       OutputTable = historyReport(occurences, legs, intermittent, consecutiveDays, ata, exclude_EqID, airline_operator, include_current_message, aircraft_no, fromDate , toDate)
       mdcDataDF=mdcDF(occurences, legs, intermittent, consecutiveDays, ata, exclude_EqID, airline_operator, include_current_message, aircraft_no, fromDate , toDate)
       resObj= jamReport(OutputTable, ACSN_chosen,mdcDataDF)
       return resObj.to_json(orient='records')

   elif (analysisType.lower() == "daily"):
        OutputTable = dailyReport(occurences, legs, intermittent, consecutiveDays, ata, exclude_EqID, airline_operator, include_current_message,aircraft_no, fromDate , toDate)
        mdcDataDF=mdcDF(occurences, legs, intermittent, consecutiveDays, ata, exclude_EqID, airline_operator, include_current_message,aircraft_no, fromDate , toDate)
        resObj= jamReport(OutputTable, ACSN_chosen,mdcDataDF)
        return resObj.to_json(orient='records')


# --------------flagReport------------------    
@app.post("/api/GenerateReport/{analysisType}/{occurences}/{legs}/{intermittent}/{consecutiveDays}/{ata}/{exclude_EqID}/{airline_operator}/{include_current_message}/{aircraft_no}/{fromDate}/{toDate}/{flag}/{list_of_tuples_acsn_bcode}")
def generateFlagReport(analysisType: str, occurences: int, legs: int, intermittent: int, consecutiveDays: int, ata: str, exclude_EqID:str, airline_operator: str, include_current_message: int, aircraft_no: str, fromDate: str , toDate: str, flag:int, list_of_tuples_acsn_bcode):

    if (analysisType.lower() == "history"):
        
        OutputTableHistory = historyReport(occurences, legs, intermittent, consecutiveDays, ata, exclude_EqID, airline_operator, include_current_message,aircraft_no, fromDate , toDate)
        print("----------this is outputtable history--------")
        print(OutputTableHistory)
        mdcDataDF=mdcDF(occurences, legs, intermittent, consecutiveDays, ata, exclude_EqID, airline_operator, include_current_message,aircraft_no, fromDate , toDate)
        print("----------this is mdcDatadf--------")
        print(mdcDataDF)
        newreport = True
        Flagsreport = 1

        resObj= Toreport(Flagsreport,OutputTableHistory,mdcDataDF,include_current_message,list_of_tuples_acsn_bcode,newreport)
        print("-----this is final result ----------")
        print(resObj)
        return resObj.to_json(orient='records')


#--------------delta Reoprt--------------
@app.post(
    "/api/GenerateReport/{analysisType}/{occurences}/{legs}/{intermittent}/{consecutiveDays}/{ata}/{exclude_EqID}/{airline_operator}/{include_current_message}/{aircraft_no}/{flag}/{prev_fromDate}/{prev_toDate}/{curr_fromDate}/{curr_toDate}")
def generateDeltaReport(analysisType: str, occurences: int, legs: int, intermittent: int, consecutiveDays: int,
                              ata: str, exclude_EqID: str, airline_operator: str, include_current_message: int,aircraft_no:str,flag,
                              prev_fromDate: str, prev_toDate: str, curr_fromDate: str, curr_toDate: str):
        if (analysisType.lower() == "history"):
            delta = deltaReport(occurences, legs, intermittent, consecutiveDays, ata, exclude_EqID, airline_operator, include_current_message, aircraft_no, prev_fromDate, prev_toDate, curr_fromDate, curr_toDate)
            return delta
        else:
            return None        




#Chart 1
@app.post("/api/chart_one/{top_n}/{aircraftNo}/{ata_main}/{fromDate}/{toDate}")
def get_ChartOneData(top_n:int, aircraftNo:str, ata_main:str, fromDate: str , toDate: str):
    chart1 =  chart_one(top_n, aircraftNo,ata_main,fromDate,toDate)
    return chart1

#Chart 2 
@app.post("/api/chart_two/{top_values}/{ata}/{fromDate}/{toDate}")
def get_ChartwoData(top_values:int, ata:str, fromDate: str , toDate: str):
    chart2 =  chart_two(top_values, ata,fromDate,toDate)
    return chart2


#Chart3
@app.post("/api/chart_three/{aircraft_no}/{equation_id}/{is_flight_phase_enabled}/{fromDate}/{toDate}")
def get_CharThreeData(aircraft_no:int, equation_id:str, is_flight_phase_enabled:int, fromDate: str , toDate: str):
    return chart3Report(aircraft_no, equation_id, is_flight_phase_enabled, fromDate, toDate)

# Chart 4
@app.post("/api/chart_four/{topCount}/{analysisType}/{occurences}/{legs}/{intermittent}/{consecutiveDays}/{ata}/{exclude_EqID}/{airline_operator}/{include_current_message}/{fromDate}/{toDate}")
def get_ChartFourData(topCount: int, analysisType: str, occurences: int, legs: int, intermittent: int, consecutiveDays: int, ata: str, exclude_EqID:str, airline_operator: str, include_current_message: int, fromDate: str , toDate: str):
    return chart4Report(occurences, legs, intermittent, consecutiveDays, ata, exclude_EqID, airline_operator, include_current_message, fromDate , toDate, topCount, analysisType)


#Chart 5
@app.post("/api/chart_five/{aircraft_no}/{equation_id}/{is_flight_phase_enabled}/{fromDate}/{toDate}")
def get_CharFiveData(aircraft_no:int, equation_id:str, is_flight_phase_enabled:int, fromDate: str , toDate: str):
    return chart5Report(aircraft_no, equation_id, is_flight_phase_enabled, fromDate, toDate)
   

#Landing chartB
@app.post("/api/Landing_Chart_B")
def get_Chart_B():
    Landing_ChartB =  Landing_chartB()
    return Landing_ChartB


    


def connect_db_MDCdata_chartb(from_dt, to_dt):
    sql = "SELECT * FROM Airline_MDC_Data WHERE DateAndTime BETWEEN '" + from_dt + "' AND '" + to_dt + "'"
    column_names = ["Aircraft", "Tail", "Flight Leg No",
                    "ATA Main", "ATA Sub", "ATA", "ATA Description", "LRU",
                    "DateAndTime", "MDC Message", "Status", "Flight Phase", "Type",
                    "Intermittent", "Equation ID", "Source", "Diagnostic Data",
                    "Data Used to Determine Msg", "ID", "Flight", "airline_id", "aircraftno"]
    print(sql)
    try:
        conn = pyodbc.connect(driver=db_driver, host=hostname, database=db_name,
                              user=db_username, password=db_password)
        MDCdataDF_chartb = pd.read_sql(sql, conn)
        MDCdataDF_chartb.columns = column_names
        conn.close()
        return MDCdataDF_chartb
    except pyodbc.Error as err:
        print("Couldn't connect to Server")
        print("Error message:- " + str(err))

#with ata chartb

def connect_to_fetch_all_ata(from_dt, to_dt):
    all_ata_query = "SELECT DISTINCT SUBSTRING(ATA, 0, CHARINDEX('-', ATA)) AS ATA_Main from MDC_MSGS WHERE MSG_Date BETWEEN '" + from_dt + "' AND '" + to_dt + "'"
    try:
        conn = pyodbc.connect(driver=db_driver, host=hostname, database=db_name,
                              user=db_username, password=db_password)
        all_ata_df = pd.read_sql(all_ata_query, conn)

        return all_ata_df
    except pyodbc.Error as err:
        print("Couldn't connect to Server")
        print("Error message:- " + str(err))
        

#stacked chart
@app.post("/api/Landing_Chart_B/{ata}/{top_n}/{from_dt}/{to_dt}")
def get_Chart_B(ata:str,top_n: int,from_dt: str, to_dt: str):
    stacked =  stacked_chart(ata, top_n,from_dt,to_dt)
    return stacked




#corelation
@app.post("/api/corelation_tail/{fromDate}/{toDate}/{equation_id}/{tail_no}/{status}")
def getCorelationDataTAIL(fromDate: str, toDate: str, equation_id, tail_no,status:int,days:Optional[int]=""):
    corelation_df = connect_database_for_corelation_tail(fromDate, toDate, equation_id, tail_no,status,days)
    corelation_df_json = corelation_df.to_json(orient='records')
    return corelation_df_json

# for reference -> http://localhost:8000/corelation/11-11-2020/11-12-2020/B1-008003/27
@app.post("/api/corelation_ata/{fromDate}/{toDate}/{status}")
def getCorelationDataATA(fromDate: str, toDate: str, status:int, equation_id:Optional[str]="", ata:Optional[str]=""):
    corelation_df = connect_database_for_corelation_ata(fromDate, toDate, status, equation_id, ata)
    corelation_df_json = corelation_df.to_json(orient='records')
    return corelation_df_json


@app.post("/api/corelation_pid/{p_id}/{status}")
def getCorelationDataPID(p_id: str,status : int):
    corelation_df = connect_database_for_corelation_pid(p_id,status)
    corelation_df_json = corelation_df.to_json(orient='records')
    return corelation_df_json


#return all equation id's
@app.post("/api/GenerateReport/equation_id/{all}")
def get_eqIData(all:str):
    f = open ('equations.json', "r")
    data = json.loads(f.read())
    data_string = json.dumps(data)
    return data_string

#return all ACSN
@app.post("/api/get_all_ACSN")
def get_eqIData():
    get_all_acsn_df = connect_database_ACSN()
    get_all_acsn_df_json = get_all_acsn_df.to_json(orient='records')
    return get_all_acsn_df_json

def connect_database_ACSN():
    sql = "SELECT DISTINCT [AC_SN] FROM [dbo].[MDC_MSGS]"
    try:
        conn = pyodbc.connect(driver=db_driver, host=hostname, database=db_name,
                              user=db_username, password=db_password)
        get_all_acsn_df = pd.read_sql(sql, conn)
        return get_all_acsn_df
    except pyodbc.Error as err:
        print("Couldn't connect to Server")
        print("Error message:- " + str(err)) 


def connect_database_for_ata_main(all):
    sql = "SELECT DISTINCT SUBSTRING(ATA, 0, CHARINDEX('-', ATA)) AS ATA_Main FROM MDC_MSGS"

    try:
        conn = pyodbc.connect(driver=db_driver, host=hostname, database=db_name,
                              user=db_username, password=db_password)
        report_ata_main_sql_df = pd.read_sql(sql, conn)
        return report_ata_main_sql_df
    except pyodbc.Error as err:
        print("Couldn't connect to Server")
        print("Error message:- " + str(err))



@app.post("/api/GenerateReport/ata_main/{all}")
def get_eqIData(all:str):
    report_ata_main_sql_df = connect_database_for_ata_main(all)
    report_ata_main_sql_df_json = report_ata_main_sql_df.to_json(orient='records')
    return report_ata_main_sql_df_json

    
#upload message input file   
@app.post("/api/uploadfile_input_message/")
def uploadfile_input_message(file: UploadFile = File(...)):
    result = insertData_MDCMessageInputs(file)
    return {"result": result} 

# update input message data    
def connect_database_for_update(Equation_ID,LRU,ATA,Message_No,Comp_ID,Message,Fault_Logged,Status,Message_Type,EICAS,Timer,Logic,Equation_Description,Occurrence_Flag,Days_Count,Priority,MHIRJ_ISE_Recommended_Action,Additional_Comments,MHIRJ_ISE_inputs,MEL_or_No_Dispatch,Keywords):
   # try:
         
        conn = pyodbc.connect(
            Trusted_Connection='No',
            driver='{ODBC Driver 17 for SQL Server}', host='aftermarket-mhirj.database.windows.net', database='MHIRJ_HUMBER',
                              user='humber_rw', password='nP@yWw@!$4NxWeK6p*ttu3q6')
        cursor = conn.cursor()  
        sql ="UPDATE MDCMessagesInputs SET  "

       
        sql +=  " [LRU]=  '" + LRU + "' "


       
        sql +=  ", [ATA]=  '" + ATA + "' " 

       
        sql += ", [Message_NO]=  '" + Message_No + "' "  

       
        sql +=  ", [Comp_ID]=  '" + Comp_ID + "' " 

       
        sql +=  ", [Message]=  '" + Message + "' "  


       
        sql +=  ", [Fault_Logged]=  '" + Fault_Logged + "' "    

       
        sql +=  ", [Status]=  '" + Status + "' "    

        
        sql +=  ", [Message_Type]=  '" + Message_Type + "' "                      
        
       
        sql +=  ", [EICAS]=  '" + EICAS + "' "

        
        sql +=  ", [Timer]=  '" + Timer + "' " 

       
        sql +=  ", [Logic]=  '" + Logic + "' " 

        
        sql +=  ", [Equation_Description]=  '" + Equation_Description + "' " 

       
        sql +=  " ,[Occurrence_Flag]=  '" + Occurrence_Flag + "' "   

       
        sql +=  ", [Days_Count]=  '" + Days_Count + "' "                    
                               
       
        sql +=  " ,[Priority] = '" + Priority + "'"


        
        sql +=  ",[MHIRJ_ISE_Recommended_Action] = '" + MHIRJ_ISE_Recommended_Action + "'"

       
        sql +=  ",[Additional_Comments] = '" + Additional_Comments + "'"         

       
        sql +=  ",[MHIRJ_ISE_inputs] = '" + MHIRJ_ISE_inputs + "'"     


       
        sql +=  ",[MEL_or_No_Dispatch] = '" + MEL_or_No_Dispatch + "'"

       
        sql +=  ",[Keywords] = '" + Keywords + "'"           
            
                     
 
        sql += "  WHERE [Equation_ID] = '" + Equation_ID + "'"
        print("---print sql builder----")
        print(sql)
 
        cursor.execute(sql)
        conn.commit()
        conn.close()
        return "Successfully UPDATE into MDCMessagesInputs"

   

@app.post("/api/update_input_message_data")
async def update_data(request: Request):
    requestData = await request.json()
    data = requestData.get('data')
    for value in data :
        print(value.get('Equation_ID'))
        print(value.get('EICAS'))
        print(value.get('Priority'))
        connect_database_for_update(value.get('Equation_ID'),value.get('LRU'),value.get('ATA'),value.get('Message_No'),value.get('Comp_ID'),value.get('Message'),value.get('Fault_Logged'),value.get('Status'),value.get('Message_Type'),value.get('EICAS'),value.get('Timer'),value.get('Logic'),value.get('Equation_Description'),value.get('Occurrence_Flag'),value.get('Days_Count'),value.get('Priority'),value.get('MHIRJ_ISE_Recommended_Action'),value.get('Additional_Comments'),value.get('MHIRJ_ISE_inputs'),value.get('MEL_or_No_Dispatch'),value.get('Keywords'))

    return "Successfully UPDATE into MDCMessagesInputs"



#all mdc message input
@app.post("/api/all_mdc_messages_input/")
def get_mdcMessageInput(eq_id: Optional[str]=""):
    mdcRaw_df = connect_database_mdc_message_input(eq_id)
    mdcRaw_df_json =  mdcRaw_df.to_json(orient='records')
    print(mdcRaw_df_json)
    return mdcRaw_df_json

@app.post("/api/testing_api/")
def testing_api():
    return {"update":1}

# delete mdc message input 
# @app.post("/api/delete_mdc_messages_input_by_eq_id/{eq_id}")
# def delete_mdc_messages_input_by_eq_id(eq_id:str):
#     mdcRaw_df = db_delete_mdc_messages_input_by_eq_id(eq_id)
#     print(mdcRaw_df)
#     return  mdcRaw_df

# insert mdc message input
# @app.post("/api/insert_mdc_messages_input/")
# def insert_mdc_messages_input(rawdata: Request):
#     try : 
#         t = await rawdata.json()
#         data = db_insert_mdc_messages_input(t)
#         return data
#     except Exception as e: 
#         print(e)
#         return {'error': 'error in '+str(e)}

#Filter
@app.post("/api/insert_filter/")
async def insert_mdc_messages_input(rawdata: Request):
    try : 
        t = await rawdata.json()
        data = db_insert_filter_data(t)
        return data
    except Exception as e: 
        print(e)
        return {'error': 'error in '+str(e)}