from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import os
import urllib

class DB_RW():
    def __init__(self):
        self.hostname = os.environ.get('hostname', 'aftermarket-mhirj.database.windows.net')
        self.db_server_port = urllib.parse.quote_plus(str(os.environ.get('db_server_port', '8002')))
        self.db_name = os.environ.get('db_name', 'MHIRJ_HUMBER')
        self.db_username = urllib.parse.quote_plus(str(os.environ.get('db_username', 'humber_rw')))
        self.db_password = urllib.parse.quote_plus(str(os.environ.get('db_password', 'nP@yWw@!$4NxWeK6p*ttu3q6')))
        self.ssl_mode = urllib.parse.quote_plus(str(os.environ.get('ssl_mode','prefer')))
        self.db_driver = "ODBC Driver 17 for SQL Server"
        self.db_rw = self.initDB()

    def initDB(self):
        db_rw = FastAPI()
        db_rw.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
        return db_rw

class App():
    
    def __init__(self):
        # self.hostname = os.environ.get('hostname', 'mhrijhumber.database.windows.net')
        # self.db_server_port = urllib.parse.quote_plus(str(os.environ.get('db_server_port', '8080')))
        # self.db_name = os.environ.get('db_name', 'MHIRJ')
        # self.db_username = urllib.parse.quote_plus(str(os.environ.get('db_username', 'mhrij')))
        # self.db_password = urllib.parse.quote_plus(str(os.environ.get('db_password', 'KaranCool123')))
        # self.ssl_mode = urllib.parse.quote_plus(str(os.environ.get('ssl_mode','prefer')))
        # self.db_driver = "ODBC Driver 17 for SQL Server"
        # self.app = self.initDB()
        self.hostname = os.environ.get('hostname', 'aftermarket-mhirj.database.windows.net')
        self.db_server_port = urllib.parse.quote_plus(str(os.environ.get('db_server_port', '8000')))
        self.db_name = os.environ.get('db_name', 'MDP-Dev')
        self.db_username = urllib.parse.quote_plus(str(os.environ.get('db_username', 'humber_ro')))
        self.db_password = urllib.parse.quote_plus(str(os.environ.get('db_password', 'Container-Zesty-Wriggly7-Catalog')))
        self.ssl_mode = urllib.parse.quote_plus(str(os.environ.get('ssl_mode','prefer')))
        self.db_driver = "ODBC Driver 17 for SQL Server"
        self.app = self.initDB()

    def initDB(self):
        app = FastAPI()
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
        return app


