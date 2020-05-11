from . pysql_config import DRIVER_CLASSES_CONFIG as driver_config, DB_DRIVER, USERNAME, DATABASENAME, PASSWORD, HOST, PORT
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
        cls.operator_is_not_null = config['SQL_OPERATORS_CLASSES']['is_not_null']
        cls.operator_or = config['SQL_OPERATORS_CLASSES']['or']
        cls.operator_in = config['SQL_OPERATORS_CLASSES']['in']
        cls.operator_not_in = config['SQL_OPERATORS_CLASSES']['not_in']
        cls.operator_like = config['SQL_OPERATORS_CLASSES']['like']
        cls.operator_not_like = config['SQL_OPERATORS_CLASSES']['not_like']
        cls.operator_less_than = config['SQL_OPERATORS_CLASSES']['less_than']
        cls.operator_bigger_than = config['SQL_OPERATORS_CLASSES']['bigger_than']
        cls.operator_exists = config['SQL_OPERATORS_CLASSES']['exists']
        cls.operator_not_exists = config['SQL_OPERATORS_CLASSES']['not_exists']

        cls.sql_command_class = config['SQL_COMMAND_CLASSES']['select_script']
        cls.insert_command_class = config['SQL_COMMAND_CLASSES']['insert_script']
        cls.update_command_class = config['SQL_COMMAND_CLASSES']['update_script']
        cls.delete_command_class = config['SQL_COMMAND_CLASSES']['delete_script']
        cls.script_executor = config['SCRIPT_EXECUTOR_CLASS']
        cls.sql_functions_config_class = config['SQL_FUNCTIONS_CLASS']
        
        
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
            'is_not_null': cls.operator_is_not_null,
            'or': cls.operator_or,
            'in': cls.operator_in,
            'not_in': cls.operator_not_in,
            'like': cls.operator_like,
            'not_like': cls.operator_not_like,
            'less_than': cls.operator_less_than,
            'bigger_than': cls.operator_bigger_than,
            'exists': cls.operator_exists,
            'not_exists': cls.operator_not_exists
        }
    
    @classmethod
    def get_command_sql_object(cls):
        cls.define_initial_data(DB_DRIVER)
        return cls.sql_command_class(cls.get_script_executor())
    
    @classmethod
    def get_command_insert_object(cls):
        cls.define_initial_data(DB_DRIVER)
        return cls.insert_command_class(cls.get_script_executor())
    
    @classmethod
    def get_command_update_object(cls):
        cls.define_initial_data(DB_DRIVER)
        return cls.update_command_class(cls.get_script_executor())
    
    @classmethod
    def get_command_delete_object(cls):
        cls.define_initial_data(DB_DRIVER)
        return cls.delete_command_class(cls.get_script_executor())    
    
    @classmethod
    def get_sql_functions_config_class(cls):
        cls.define_initial_data(DB_DRIVER)
        return cls.sql_functions_config_class

    @classmethod
    def get_script_executor(cls):
        cls.define_initial_data(DB_DRIVER)
        return cls.script_executor(databasename=DATABASENAME, username=USERNAME, password=PASSWORD, host=HOST, port=PORT)

