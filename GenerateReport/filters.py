from statistics import mode
from app2 import App2
import pyodbc
import pandas as pd
from datetime import datetime
import json

from model.filtermodel import FilterModel

driver_vdi = '{ODBC Driver 17 for SQL Server}'
host_vdi='aftermarket-mhirj.database.windows.net'
database_vdi='MHIRJ_HUMBER'
user_vdi='humber_rw'
password_vdi='nP@yWw@!$4NxWeK6p*ttu3q6'

def db_insert_filter_data(data):
    try : 
        model = FilterModel(
            data['user_id'],
            data['filter_name'],
            data['filter_data']
        )
        print('db_insert_filter_data data ',model.filter_data)
        conn = pyodbc.connect(
            Trusted_Connection='No',
            driver=driver_vdi, host=host_vdi, database=database_vdi,
                              user=user_vdi, password=password_vdi)


        cursor = conn.cursor()       
        cursor.execute('''
        IF NOT EXISTS (SELECT * FROM dbo.Users_Filters WHERE FILTER_NAME = ?)
        INSERT INTO dbo.Users_Filters(USER_ID,FILTER_NAME,FILTER_DATA)
        VALUES(?,?,?)
        ELSE
        UPDATE dbo.Users_Filters
        SET FILTER_DATA = ?
        WHERE FILTER_NAME = ?
       
       ''',
       model.filter_name,
       model.user_id,
       model.filter_name,
       model.filter_data,
        model.filter_data,
         model.filter_name,
       )
        conn.commit()
        return {"status":True,"message":"Data Updated Successfully."}
    except Exception as e: 
        print(e)
        return {'status':False,'message': 'error in '+str(e)}