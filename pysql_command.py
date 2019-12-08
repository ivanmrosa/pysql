from pysql_class_generator import PySqlClassGenerator


def select(table):
   sql = PySqlClassGenerator.get_command_sql_object()
   sql.add_operation('SELECT', table)
   sql.script_fields = 'SELECT'
   sql.script_from = ' FROM {table} {alias}'.format(table=table.get_db_name(), alias=table.get_alias())
   return sql

def update(table):
    pass

def insert(table):
    pass

def delele(table):
    pass

