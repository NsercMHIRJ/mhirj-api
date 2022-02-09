from app import App
import pyodbc
import pandas as pd 
import datetime

App().app
def connect_to_fetch_all_ata(from_dt, to_dt):
    conn = pyodbc.connect(driver=App().db_driver, host=App().hostname, database=App().db_name,
                              user=App().db_username, password=App().db_password)
    all_ata_query = "SELECT DISTINCT SUBSTRING(ATA, 0, CHARINDEX('-', ATA)) AS ATA_Main from MDC_MSGS WHERE MSG_Date BETWEEN '" + from_dt + "' AND '" + to_dt + "'"
    try:
        all_ata_df = pd.read_sql(all_ata_query, conn)

        return all_ata_df
    except pyodbc.Error as err:
        print("Couldn't connect to Server")
        print("Error message:- " + str(err))


def fetch_all_eqids(from_dt, to_dt):
    conn = pyodbc.connect(driver=App().db_driver, host=App().hostname, database=App().db_name,
                              user=App().db_username, password=App().db_password)
    all_ata_query = "SELECT DISTINCT EQ_ID from MDC_MSGS WHERE MSG_Date BETWEEN '" + from_dt + "' AND '" + to_dt + "'"
    try:
        all_eqid_df = pd.read_sql(all_ata_query, conn)

        return all_eqid_df
    except pyodbc.Error as err:
        print("Couldn't connect to Server")
        print("Error message:- " + str(err))

# def connect_database_for_chart3(aircraft_no, equation_id, CurrentFlightPhaseEnabled, from_dt, to_dt):


#     if CurrentFlightPhaseEnabled == 0: # Flight phase is NOT enabled
#         sql = "SELECT COUNT(*) AS OccurencesPerDay, cast(DateAndTime as DATE) AS Dates from Airline_MDC_Data WHERE Equation_ID='"+equation_id+"' AND aircraftno = '"+str(aircraft_no)+"' AND Flight_Phase IS NOT NULL AND DateAndTime BETWEEN '"+from_dt+"' AND '"+to_dt+"' GROUP BY cast(DateAndTime as DATE)"
#     elif CurrentFlightPhaseEnabled == 1:
#         sql = "SELECT COUNT(*) AS OccurencesPerDay, cast(DateAndTime as DATE) AS Dates from Airline_MDC_Data WHERE Equation_ID='"+equation_id+"' AND aircraftno = '"+str(aircraft_no)+"' AND Flight_Phase IS NULL AND DateAndTime BETWEEN '"+from_dt+"' AND '"+to_dt+"' GROUP BY cast(DateAndTime as DATE)"
#     print(sql)
#     try:
#         conn = pyodbc.connect(driver=App().db_driver, host=App().hostname, database=App().db_name,
#                               user=App().db_username, password=App().db_password)
#         chart3_sql_df = pd.read_sql(sql, conn)
#         # MDCdataDF.columns = column_names
#         conn.close()
#         return chart3_sql_df
#     except pyodbc.Error as err:
#         print("Couldn't connect to Server")
#         print("Error message:- " + str(err))


def connect_database_for_charts(aircraft_no, equation_id, CurrentFlightPhaseEnabled, from_dt, to_dt):
    conn = pyodbc.connect(driver=App().db_driver, host=App().hostname, database=App().db_name,
                              user=App().db_username, password=App().db_password)
    if equation_id == 'NONE':
        all_eqid = fetch_all_eqids(from_dt, to_dt)

        all_eqid_str = "("
        all_eqid_list = all_eqid['EQ_ID'].tolist()
        for each_eqid in all_eqid_list:
            #all_eqid_str_list.append(str(each_eqid))
            all_eqid_str += "'" + str(each_eqid) + "'"
            if each_eqid != all_eqid_list[-1]:
                all_eqid_str += ","
            else:
                all_eqid_str += ")"

        equation_id = all_eqid_str
    
    if CurrentFlightPhaseEnabled == 0:
        sql = "SELECT * FROM MDC_MSGS WHERE EQ_ID NOT IN " + equation_id + " AND AC_SN = " + str(aircraft_no) + " AND MSG_Date BETWEEN '" + from_dt + "' AND '" + to_dt + "'"
    else:
        sql = "SELECT * FROM MDC_MSGS WHERE EQ_ID IN " + equation_id + " AND AC_SN = " + str(aircraft_no) + " AND MSG_Date BETWEEN '" + from_dt + "' AND '" + to_dt + "'"

    print('Chart3 SQL ', sql)
    try:
        chart3_sql_df = pd.read_sql(sql, conn)
        # conn.close()
        return chart3_sql_df
    except pyodbc.Error as err:
        print("Couldn't connect to Server")
        print("Error message:- " + str(err))



## Chart 2
def connect_database_for_chart2(n, ata, from_dt, to_dt):
    if len(ata) == 2:
        sql = "SELECT AC_SN,AC_TN,ATA,SUBSTRING(ATA, 0, CHARINDEX('-', ATA)) AS ATA_Main FROM MDC_MSGS WHERE SUBSTRING(ATA, 0, CHARINDEX('-', ATA))="+ata+" AND MSG_Date BETWEEN '"+from_dt+"' AND '"+to_dt+"'"
    elif len(ata) == 5:  
       sql = "SELECT AC_SN,AC_TN,ATA,SUBSTRING(ATA, 0, CHARINDEX('-', ATA)) AS ATA_Main FROM MDC_MSGS where  ATA='"+ata+"'   AND MSG_Date BETWEEN '"+from_dt+"' AND '"+to_dt+"' "
  
    column_names = ["AC_MODEL", "AC_SN", "AC_TN",
                    "OPERATOR", "MSG_TYPE", "MDC_SOFTWARE", "MDT_VERSION", "MSG_Date",
                    "FLIGHT_NUM","FLIGHT_LEG", "FLIGHT_PHASE", "ATA", "ATA_NAME", "LRU",
                    "COMP_ID", "MSG_TXT","EQ_ID", "INTERMITNT", "EVENT_NOTE",
                    "EQ_TS_NOTE","SOURCE", "MSG_ID", "FALSE_MSG","BOOKMARK","msg_status"]
    print(sql)
  
    try:
       conn = pyodbc.connect(driver=App().db_driver, host=App().hostname, database=App().db_name,
                              user=App().db_username, password=App().db_password)
       chart2_sql_df = pd.read_sql(sql, conn)
       # MDCdataDF.columns = column_names
       # chart2_sql_df.columns = column_names
 
       conn.close()
       return chart2_sql_df
    except pyodbc.Error as err:
       print("Couldn't connect to Server")
       print("Error message:- " + str(err))
