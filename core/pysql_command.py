from . pysql_class_generator import PySqlClassGenerator


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
    insert_object.table = table
    return insert_object

def delete(table):
    delete_object = PySqlClassGenerator.get_command_delete_object()
    delete_object.add_operation('DELETE', table)
    delete_object.table = table
    return delete_object

