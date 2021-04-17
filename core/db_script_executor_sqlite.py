import os, sqlite3
from . interface import PySqlRunScriptInterface


class SqliteScriptExecutor(PySqlRunScriptInterface):
    def __init__(self, databasename, username, password, host, port, debug = False):
        self.connector = None
        self.cursor = None   
        self.__databasename = databasename
        self.__username = username
        self.__password = password     
        self.__host = host
        self.__port = port
        self.__debug = debug
    
    def print_log(self, text):
        if self.__debug:
            print(text)

    def open_connection(self):
        if not self.connector:
            self.connector = sqlite3.connect(self.__databasename)
        if not self.cursor:
            self.cursor = self.connector.cursor()
    
    def close_connection(self):
        if self.cursor:
            self.cursor.close()
            self.cursor = None
        if self.connector:
            self.connector.close()
            self.connector = None
    
    def execute_ddl_script(self, script, auto_commit=False):    
        self.print_log(script) 
        if script:   
            self.open_connection()
            print(script)
            self.cursor.execute(script)
            
            if auto_commit:
                self.connector.commit()
                self.close_connection()


    def execute_dml_script(self, script, params, auto_commit = False):
        self.print_log(script)
        if script:               
            self.open_connection()
            self.cursor.execute(script, params)
            
            if auto_commit:
                self.connector.commit()        
                self.close_connection()

    def execute_select_script(self, sql, params):
        self.print_log(sql) 
        self.open_connection()
        self.cursor.execute(sql, params)
        results =  self.cursor.fetchall()
        self.close_connection()
        return results
    
    def commit(self):
        if self.connector:
            self.connector.commit()
            self.close_connection()
    
    def run_ddl_isolated(self, script, databasename):
        self.print_log(script) 
        self.close_connection()
        original_database = self.__databasename        
        self.__databasename = databasename    
        try:
            self.open_connection()           
            self.execute_ddl_script(script)
            self.close_connection()
        finally:            
            self.__databasename = original_database
            self.close_connection()


    def create_database(self):  
        try:
            self.open_connection()            
        finally:
            self.close_connection()
            
                
    def drop_database(self):
        self.close_connection()
        if os.path.exists(self.__databasename):
            os.remove(self.__databasename)

    def __del__(self):
        self.close_connection()


#class_script_executor = None

#if DB_DRIVER == POSTGRESQL:
#    class_script_executor = PostgreScriptExecutor

#ScriptExecutor = type("ScriptExecutor", (class_script_executor, ), {})