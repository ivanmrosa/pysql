import os, json

BASE_DIR = os.getcwd()

with open(os.path.join(BASE_DIR, 'pysql_config.json'), 'r') as f:
    DB_CONFIG = json.loads(f.read())

DB_DRIVER = DB_CONFIG['driver']
HOST = DB_CONFIG['host']
PORT = DB_CONFIG['port']
DATABASENAME = DB_CONFIG['databasename']
PASSWORD = DB_CONFIG['password']
USERNAME = DB_CONFIG['username']

if 'debug' in DB_CONFIG:
    DEBUG = DB_CONFIG['debug']
else:
    DEBUG = False

POSTGRESQL = 'POSTGRESQL'
ORACLE = 'ORACLE'
MYSQL = 'MYSQL'
SQLSERVER = 'SQLSERVER'
SQLITE = 'SQLITE'


from . pysql_db_tables_definitions import PostgreDbTable, OracleDbTable, MySqlDbTable, SqlServerDbTable, SqliteDbTable

from . pysql_operations_base import GenericOequPostgre, GenericOdifPostgre, GenericOnullPostgre, GenericOorPostgre, GenericOinPostgre, \
    GenericOninPostgre, GenericOlikePostgre, GenericOnlikePostgre, GenericOnnullPostgre, GenericObtPostgre, GenericOltPostgre, \
    GenericOexPostgre, GenericOnexPostgre,  GenericOequSqlite, GenericOdifSqlite, GenericOnullSqlite, GenericOorSqlite, GenericOinSqlite, \
    GenericOninSqlite, GenericOlikeSqlite, GenericOnlikeSqlite, GenericOnnullSqlite, GenericObtSqlite, GenericOltSqlite, \
    GenericOexSqlite, GenericOnexSqlite

from . pysql_command_base import GenericBaseDmlSelectPostgre, GenericBaseDmlSelectMySql,  GenericBaseDmlSelectOracle, \
    GenericBaseDmlSelectSqlServer,  GenericBaseDmlSelectSqlite,  GenericBaseDmlInsertPostgre, GenericBaseDmlUpdatePostgre, GenericBaseDmlUpdateMySql, \
    GenericBaseDmlUpdateOracle, GenericBaseDmlUpdateSqlServer, GenericBaseDmlInsertSqlite, GenericBaseDmlUpdateSqlite, GenericBaseDmlDeletePostgre, GenericBaseDmlDeleteMySql, \
    GenericBaseDmlDeleteOracle, GenericBaseDmlDeleteSqlServer, GenericBaseDmlDeleteSqlite

from . pysql_functions_config import PysqlFunctionsConfigPostgre, PysqlFunctionsConfigOracle, PysqlFunctionsConfigSqlServer, PysqlFunctionsConfigMySql, PysqlFunctionsConfigSqlite

script_executor_class = None

if DB_DRIVER == POSTGRESQL:
    from . db_script_executor_postgre import PostgreScriptExecutor
    script_executor_class = PostgreScriptExecutor
elif DB_DRIVER == SQLITE:
    from . db_script_executor_sqlite import SqliteScriptExecutor
    script_executor_class = SqliteScriptExecutor


DRIVER_CLASSES_CONFIG = {
   POSTGRESQL : {
       'DB_TABLE_CLASS': PostgreDbTable,
       'SCRIPT_EXECUTOR_CLASS': script_executor_class,
       'SQL_FUNCTIONS_CLASS' : PysqlFunctionsConfigPostgre,
       'SQL_OPERATORS_CLASSES': {
           'equal': GenericOequPostgre,
           'different': GenericOdifPostgre,
           'isNull': GenericOnullPostgre,
           'is_not_null': GenericOnnullPostgre,
           'or': GenericOorPostgre,
           'in': GenericOinPostgre,
           'not_in': GenericOninPostgre,
           'like': GenericOlikePostgre,
           'not_like': GenericOnlikePostgre,
           'less_than': GenericOltPostgre,
           'bigger_than': GenericObtPostgre,
           'exists': GenericOexPostgre,
           'not_exists': GenericOnexPostgre       
       },
       'SQL_COMMAND_CLASSES': {
          'select_script': GenericBaseDmlSelectPostgre,
          'insert_script': GenericBaseDmlInsertPostgre,
          'update_script': GenericBaseDmlUpdatePostgre,
          'delete_script': GenericBaseDmlDeletePostgre
       },       
       'FIELDS_CONFIG':{
            'IntegerField': {'NAME' : 'INTEGER', 'HAS_SIZE': False, 'HAS_SCALE': False, 'HAS_PRECISION': False},
            'SmallIntField': {'NAME': 'SMALLINT', 'HAS_SIZE': False, 'HAS_SCALE': False, 'HAS_PRECISION': False},
            'BigIntField': {'NAME': 'BIGINT', 'HAS_SIZE': False, 'HAS_SCALE': False, 'HAS_PRECISION': False},
            'NumericField': {'NAME': 'NUMERIC', 'HAS_SIZE': False, 'HAS_SCALE': True, 'HAS_PRECISION': True},
            'FloatField': {'NAME': 'REAL', 'HAS_SIZE': False, 'HAS_SCALE': False, 'HAS_PRECISION': False},
            'MoneyField': {'NAME': 'MONEY', 'HAS_SIZE': False, 'HAS_SCALE': False, 'HAS_PRECISION': False},            
            'CharacterField' : {'NAME': 'CHAR', 'HAS_SIZE': True, 'HAS_SCALE': False, 'HAS_PRECISION': False},
            'VarcharField': {'NAME': 'VARCHAR', 'HAS_SIZE': True, 'HAS_SCALE': False, 'HAS_PRECISION': False},
            'TextField' : {'NAME': 'TEXT', 'HAS_SIZE': False, 'HAS_SCALE': False, 'HAS_PRECISION': False},
            'IntegerPrimaryKey' : {'NAME' : 'SERIAL', 'HAS_SIZE': False, 'HAS_SCALE': False, 'HAS_PRECISION': False},
            'DateField' : {'NAME' : 'DATE', 'HAS_SIZE': False, 'HAS_SCALE': False, 'HAS_PRECISION': False},     
            'DateTimeField' : {'NAME' : 'TIMESTAMP', 'HAS_SIZE': False, 'HAS_SCALE': False, 'HAS_PRECISION': False},     
       },

       'CURRENT_DATE_TYPE': 'CURRENT_DATE'       
   },

    SQLITE : {
       'DB_TABLE_CLASS': SqliteDbTable,
       'SCRIPT_EXECUTOR_CLASS': script_executor_class,
       'SQL_FUNCTIONS_CLASS' : PysqlFunctionsConfigSqlite,
       'SQL_OPERATORS_CLASSES': {
           'equal': GenericOequSqlite,
           'different': GenericOdifSqlite,
           'isNull': GenericOnullSqlite,
           'is_not_null': GenericOnnullSqlite,
           'or': GenericOorSqlite,
           'in': GenericOinSqlite,
           'not_in': GenericOninSqlite,
           'like': GenericOlikeSqlite,
           'not_like': GenericOnlikeSqlite,
           'less_than': GenericOltSqlite,
           'bigger_than': GenericObtSqlite,
           'exists': GenericOexSqlite,
           'not_exists': GenericOnexSqlite       
       },
       'SQL_COMMAND_CLASSES': {
          'select_script': GenericBaseDmlSelectSqlite,
          'insert_script': GenericBaseDmlInsertSqlite,
          'update_script': GenericBaseDmlUpdateSqlite,
          'delete_script': GenericBaseDmlDeleteSqlite
       },
       'FIELDS_CONFIG':{
            'IntegerField': {'NAME' : 'INTEGER', 'HAS_SIZE': False, 'HAS_SCALE': False, 'HAS_PRECISION': False},
            'SmallIntField': {'NAME': 'SMALLINT', 'HAS_SIZE': False, 'HAS_SCALE': False, 'HAS_PRECISION': False},
            'BigIntField': {'NAME': 'BIGINT', 'HAS_SIZE': False, 'HAS_SCALE': False, 'HAS_PRECISION': False},
            'NumericField': {'NAME': 'NUMERIC', 'HAS_SIZE': False, 'HAS_SCALE': True, 'HAS_PRECISION': True},
            'FloatField': {'NAME': 'REAL', 'HAS_SIZE': False, 'HAS_SCALE': False, 'HAS_PRECISION': False},            
            'MoneyField': {'NAME': 'MONEY', 'HAS_SIZE': False, 'HAS_SCALE': False, 'HAS_PRECISION': False},
            'CharacterField' : {'NAME': 'CHAR', 'HAS_SIZE': True, 'HAS_SCALE': False, 'HAS_PRECISION': False},
            'VarcharField': {'NAME': 'VARCHAR', 'HAS_SIZE': True, 'HAS_SCALE': False, 'HAS_PRECISION': False},
            'TextField' : {'NAME': 'TEXT', 'HAS_SIZE': False, 'HAS_SCALE': False, 'HAS_PRECISION': False},
            'IntegerPrimaryKey' : {'NAME' : 'INTEGER', 'HAS_SIZE': False, 'HAS_SCALE': False, 'HAS_PRECISION': False},
            'DateField' : {'NAME' : 'DATE', 'HAS_SIZE': False, 'HAS_SCALE': False, 'HAS_PRECISION': False},     
            'DateTimeField' : {'NAME' : 'DATE', 'HAS_SIZE': False, 'HAS_SCALE': False, 'HAS_PRECISION': False},     
       },

       'CURRENT_DATE_TYPE': 'CURRENT_DATE'       
   }
}