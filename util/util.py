from app2 import App2
import pyodbc
import pandas as pd


def connect_database_mdc_message_input(eq_id):
    sql = "SELECT * from [dbo].[MDCMessagesInputs] c WHERE c.Equation_ID='" + eq_id + "' "

    try:
        conn = pyodbc.connect(
            Trusted_Connection='No',
            driver='{ODBC Driver 17 for SQL Server}', host='aftermarket-mhirj.database.windows.net', database='MHIRJ_HUMBER',
                              user='humber_rw', password='nP@yWw@!$4NxWeK6p*ttu3q6')
                              
        # conn = pyodbc.connect(
        #     Trusted_Connection='No',
        #     driver=App2().db_driver, host=App2().hostname, database=App2().db_name,
        #                       user=App2().db_username, password=App2().db_password)
                              
        print(sql)
        mdcRaw_df = pd.read_sql(sql, conn)
        return mdcRaw_df
    except pyodbc.Error as err:
        print("Couldn't connect to Server")
        print("Error message:- " + str(err))
        
