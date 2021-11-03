#from pysql_config import DB_DRIVER, POSTGRESQL, ORACLE, MYSQL, SQLSERVER, HOST, DATABASENAME, PASSWORD, USERNAME
from . interface import PySqlRunScriptInterface

#if DB_DRIVER == POSTGRESQL:
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.errors import DuplicateDatabase 


class PostgreScriptExecutor(PySqlRunScriptInterface):
    def __init__(self, databasename, username, password, host, port, debug = False):
        self.connector = None
        self.cursor = None   
        self.__databasename = databasename
        self.__username = username
        self.__password = password     
        self.__host = host
        self.__port = port
        self.__debug = debug
    
    def print_log(self, script):
        if self.__debug:
            print(script)
        
    def open_connection(self):
        if not self.connector:
            self.connector = psycopg2.connect("dbname={dbname} user={username} host={host} password={password} port={port}".\
                format(dbname=self.__databasename, username=self.__username, \
                host=self.__host, password=self.__password, port=self.__port))
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
        self.open_connection()
        self.cursor.execute(script)

        if auto_commit:
            self.connector.commit()
            self.close_connection()


    def execute_dml_script(self, script, params, auto_commit = False):        
        self.print_log(script)
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
        #self.close_connection()
        return results
    
    def commit(self):
        if self.connector:
            self.connector.commit()
            self.close_connection()
    
    def rollback(self):
        if self.connector:
            result = self.connector.rollback()
            self.close_connection()
            return result
        else:
            return None
    
    def run_ddl_isolated(self, script, databasename):
        self.print_log(script)                
        self.close_connection()
        original_database = self.__databasename        
        self.__databasename = databasename    
        try:
            self.open_connection()           
            self.connector.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            self.execute_ddl_script(script)
            self.close_connection()
        finally:            
            self.__databasename = original_database
            self.close_connection()


    def create_database(self):  
        try:
            self.run_ddl_isolated('create database {database};'.format(database=self.__databasename), 'postgres')   
        except Exception as identifier:                
            if type(identifier) == DuplicateDatabase:
                pass
            else:
                raise            
                
    def drop_database(self):
        self.run_ddl_isolated('drop database if exists {database};'.format(database=self.__databasename), 'postgres')   

    def __del__(self):        
        self.close_connection()


#class_script_executor = None

#if DB_DRIVER == POSTGRESQL:
#    class_script_executor = PostgreScriptExecutor

#ScriptExecutor = type("ScriptExecutor", (class_script_executor, ), {})