from . pysql_class_generator import PySqlClassGenerator
from . sql_operators import oequ
from . pysql_functions import fmax
from . interface import PySqlDatabaseTableInterface, PySqlFieldInterface
import inspect


def select(table):
   sql_command = PySqlClassGenerator.get_command_sql_object()
   sql_command.add_operation('SELECT', table)
   sql_command.script_fields = 'SELECT'
   sql_command.script_from = ' FROM {table} {alias}'.format(table=table.get_db_name(), alias=table.get_alias())
   return sql_command

def update(table):
    update_object = PySqlClassGenerator.get_command_update_object()
    update_object.add_operation('UPDATE', table)
    update_object.table = table
    return update_object

def insert(table):
    insert_object = PySqlClassGenerator.get_command_insert_object()    
    if  isinstance(table, PySqlDatabaseTableInterface) or (inspect.isclass(table) and issubclass(table, PySqlDatabaseTableInterface)):        
        insert_object.table = table
        insert_object.select_executor = select
        insert_object.oequ_clause = oequ
        insert_object.max_func = fmax        
    else:
        if issubclass(type(table), PySqlFieldInterface) and table.is_many_to_many():
            pk = table.get_owner().get_pk_fields()[0]
            if not pk.value:
                raise Exception('The value for primary key field must be informed.')
            insert_object = PySqlClassGenerator.get_command_insert_object()
            insert_object.table = table
            insert_object.select_executor = select
            insert_object.oequ_clause = oequ
            insert_object.max_func = fmax
            insert_object.just_insert_many_to_many = True
            insert_object.id_to_insert_many_to_many = pk.value
        else:
            raise Exception('Not a valid class given.')
    return insert_object

def delete(table):
    if inspect.isclass(table) and issubclass(table, PySqlDatabaseTableInterface):
        delete_object = PySqlClassGenerator.get_command_delete_object()
        delete_object.add_operation('DELETE', table)
        delete_object.table = table
        return delete_object
    else:
        if issubclass(type(table), PySqlFieldInterface) and table.is_many_to_many():

            delete_object = PySqlClassGenerator.get_command_delete_object()
            #table.set_alias(table.get_middle_class().get_db_name() + '_2')
            delete_object.add_operation('DELETE', table.get_middle_class())
            delete_object.table = table.get_middle_class()         
            return delete_object.join(table.get_owner()).join(table.get_related_to_class(), table.get_middle_class())            
        else:
            raise Exception('Not a valid class given.')


def runSql(sql, fieldNames, params=[]):
    sql_command = PySqlClassGenerator.get_command_open_sql_object()
    
    return sql_command.execute(sql.replace('::s', PySqlClassGenerator.get_command_sql_object().get_param_representation()),\
        params, fieldNames)