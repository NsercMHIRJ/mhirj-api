from Charts.helper import connect_database_for_chart1
import pandas as pd
import json
import numpy as np

def chart_one(top_n, aircraftNo,ata_main, fromDate,toDate):
# @app.post("/api/chart_one/{top_n}/{aircraftNo}/{ata_main}/{fromDate}/{toDate}")
# async def get_ChartOneData(top_n:int, aircraftNo:int, ata_main:str, fromDate: str , toDate: str):
    chart1_sql_df = connect_database_for_chart1(top_n, aircraftNo, ata_main, fromDate, toDate)
    chart1_sql_df_json = chart1_sql_df.to_json(orient='records')
    return chart1_sql_df_json