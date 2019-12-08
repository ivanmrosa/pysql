from pysql_config import DRIVER_CLASSES_CONFIG as driver_config, DB_DRIVER, USERNAME, DATABASENAME, PASSWORD, HOST, PORT
import inspect

class PySqlClassGenerator(object):
    driver = None
    db_table_class = None
    
    operator_different = None
    operator_equal = None
    operator_is_null = None
    operator_or = None

    sql_command_class = None

    script_executor = None

    initialized = False

    @classmethod
    def define_initial_data(cls, driver_name):
        if not cls.initialized:
            cls.driver = driver_name
            cls.define_classes()
            cls.initialized = True
            
    @classmethod
    def define_classes(cls):
        config = driver_config[cls.driver.upper()]
        cls.db_table_class = config['DB_TABLE_CLASS']
        cls.operator_different = config['SQL_OPERATORS_CLASSES']['different']
        cls.operator_equal = config['SQL_OPERATORS_CLASSES']['equal']
        cls.operator_is_null = config['SQL_OPERATORS_CLASSES']['isNull']
        cls.operator_or = config['SQL_OPERATORS_CLASSES']['or']
        
        cls.sql_command_class = config['SQL_COMMAND_CLASSES']['select_script']
        cls.script_executor = config['SCRIPT_EXECUTOR_CLASS']
    
    
    @classmethod
    def get_db_table_class(cls):
        cls.define_initial_data(DB_DRIVER)
        return cls.db_table_class
    
    @classmethod
    def get_db_operators_classes(cls):
        cls.define_initial_data(DB_DRIVER)
        return {
            'different': cls.operator_different,
            'equal': cls.operator_equal,
            'is_null': cls.operator_is_null,
            'or': cls.operator_or
        }
    
    @classmethod
    def get_command_sql_object(cls):
        cls.define_initial_data(DB_DRIVER)
        return cls.sql_command_class(cls.get_script_executor())

        
    @classmethod
    def get_script_executor(cls):
        cls.define_initial_data(DB_DRIVER)
        return cls.script_executor(databasename=DATABASENAME, username=USERNAME, password=PASSWORD, host=HOST, port=PORT)

