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
POSTGRESQL = 'POSTGRESQL'
ORACLE = 'ORACLE'
MYSQL = 'MYSQL'
SQLSERVER = 'SQLSERVER'


from pysql_db_tables_definitions import PostgreDbTable, OracleDbTable, MySqlDbTable, SqlServerDbTable
from pysql_operations_base import GenericOequPostgre, GenericOdifPostgre, GenericOnullPostgre, GenericOorPostgre, GenericOinPostgre, \
    GenericOninPostgre
from pysql_command_base import GenericBaseDmlSelectPostgre, GenericBaseDmlSelectMySql,  GenericBaseDmlSelectOracle, \
    GenericBaseDmlSelectSqlServer, GenericBaseDmlInsertPostgre

script_executor_class = None

if DB_DRIVER == POSTGRESQL:
    from db_script_executor_postgre import PostgreScriptExecutor
    script_executor_class = PostgreScriptExecutor



DRIVER_CLASSES_CONFIG = {
   POSTGRESQL : {
       'DB_TABLE_CLASS': PostgreDbTable,
       'SCRIPT_EXECUTOR_CLASS': script_executor_class,
       'SQL_OPERATORS_CLASSES': {
           'equal': GenericOequPostgre,
           'different': GenericOdifPostgre,
           'isNull': GenericOnullPostgre,
           'or': GenericOorPostgre,
           'in': GenericOinPostgre,
           'not_in': GenericOninPostgre
       },
       'SQL_COMMAND_CLASSES': {
          'select_script': GenericBaseDmlSelectPostgre,
          'insert_script': GenericBaseDmlInsertPostgre
       },
       'FIELDS_CONFIG':{
            'IntegerField': {'NAME' : 'INTEGER', 'HAS_SIZE': False, 'HAS_SCALE': False, 'HAS_PRECISION': False},
            'SmallIntField': {'NAME': 'SMALLINT', 'HAS_SIZE': False, 'HAS_SCALE': False, 'HAS_PRECISION': False},
            'BigIntField': {'NAME': 'BIGINT', 'HAS_SIZE': False, 'HAS_SCALE': False, 'HAS_PRECISION': False},
            'NumericField': {'NAME': 'NUMERIC', 'HAS_SIZE': False, 'HAS_SCALE': True, 'HAS_PRECISION': True},
            'MoneyField': {'NAME': 'MONEY', 'HAS_SIZE': False, 'HAS_SCALE': False, 'HAS_PRECISION': False},
            'CharacterField' : {'NAME': 'CHAR', 'HAS_SIZE': True, 'HAS_SCALE': False, 'HAS_PRECISION': False},
            'VarcharField': {'NAME': 'VARCHAR', 'HAS_SIZE': True, 'HAS_SCALE': False, 'HAS_PRECISION': False},
            'TextField' : {'NAME': 'TEXT', 'HAS_SIZE': False, 'HAS_SCALE': False, 'HAS_PRECISION': False},
            'IntegerPrimaryKey' : {'NAME' : 'SERIAL', 'HAS_SIZE': False, 'HAS_SCALE': False, 'HAS_PRECISION': False}     
       }       
   }
}