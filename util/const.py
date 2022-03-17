hostname = 'aftermarket-mhirj.database.windows.net'
db_name =  'MDP-Dev'
db_username =  'humber_ro'
db_password = 'Container-Zesty-Wriggly7-Catalog'   

db_name_mdcInput =  'MHIRJ_HUMBER'
db_username_mdcInput =  'humber_rw'
db_password_mdcInput = 'nP@yWw@!$4NxWeK6p*ttu3q6'   

connectionStringMDC = "DRIVER={ODBC Driver 17 for SQL Server};SERVER="+hostname+";DATABASE="+db_name+";UID="+db_username+";PWD="+db_password+""

connectionStringMDCInput = "DRIVER={ODBC Driver 17 for SQL Server};SERVER="+hostname+";DATABASE="+db_name_mdcInput+";UID="+db_username_mdcInput+";PWD="+db_password_mdcInput+""