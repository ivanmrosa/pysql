from field_tools import FieldTools
#from sql_db_tables import BaseDbTable 
from interface import PySqlDatabaseTableInterface
import inspect
#from db_script_executor import ScriptExecutor


class GenricBaseDmlScripts(object):
    def __init__(self, script_executor_object):
        self.list_operations = []
        self.list_params = []
        self.script_fields = ''
        self.script_from = ''
        self.script_where = ''
        self.script_executor = script_executor_object

    
    def add_operation(self, type, table):
        self.list_operations.append({"type": type, "table" : table})
    
    def join(self, table, tuple_fields_comparations = ()):
        self.script_from += ' JOIN {table} {alias} ON '.format(table=table.get_db_name(), alias=table.get_alias())
        if len(tuple_fields_comparations) == 0:
            if len(self.list_operations) > 0:
                prior_table = self.list_operations[-1:][0]["table"]
                fk_fields = table.get_fk_fields()
                count = 0
                temp_script = ''
                for fk_field in fk_fields:
                    if issubclass(fk_field.get_related_to_class(), prior_table):
                        if count == 0:
                             temp_script = '{alias_prior}.{field_name_prior} = {alias}.{field_name} '
                        else:
                            temp_script = 'AND {alias_prior}.{field_name_prior} = {alias}.{field_name} '
                        
                        self.script_from += temp_script.format(alias_prior=prior_table.get_alias(), 
                            field_name_prior=fk_field.get_related_to_class().get_pk_fields()[0].get_db_name(),
                            alias=table.get_alias(), field_name=fk_field.get_db_name())
                        count += 1
                
                if count == 0:
                    fk_fields = prior_table.get_fk_fields()
                    count = 0
                    for fk_field in fk_fields:
                        if issubclass(fk_field.get_related_to_class(), table):
                            if count == 0:
                                temp_script = '{alias_prior}.{field_name_prior} = {alias}.{field_name} '
                            else:
                                temp_script = 'AND {alias_prior}.{field_name_prior} = {alias}.{field_name} '

                            self.script_from += temp_script. \
                                format(alias_prior=prior_table.get_alias(), 
                                    field_name_prior=fk_field.get_db_name(),
                                    alias=table.get_alias(), field_name=table.get_pk_fields()[0].get_db_name())
                            count += 1
        
            else:
                raise Exception('Join must be preceded for an operator update or select')
        self.script_from = self.script_from.strip()
        self.add_operation('JOIN', table)
        return self
    
    def filter(self, *operators):
        
        if not self.script_where:
            self.script_where += ' WHERE '
        else:
            self.script_where += ' AND '
        
        temporary_script = ''

        for operator in operators:
            if operator.get_operator() == 'OR':
                if temporary_script: 
                    temporary_script += ' OR ' + operator.get_sql_text(value_as_parameter=True, list_of_parameters=self.list_params)
                else:
                    temporary_script = operator.get_sql_text(value_as_parameter=True, list_of_parameters=self.list_params)    
            else:                
                if temporary_script: 
                    temporary_script += ' AND ' + operator.get_sql_text(value_as_parameter=True, list_of_parameters=self.list_params)
                else:
                    temporary_script += operator.get_sql_text(value_as_parameter=True, list_of_parameters=self.list_params)
                    
        self.script_where += temporary_script

        return self


class GenricBaseDmlSelect(GenricBaseDmlScripts):
    def get_sql(self, *fields):
        str_fields = ''
        
        if fields:
            for field in fields:
                #tuple means field with alias (Table.field, 'field_alias')
                is_class = inspect.isclass(field)
                is_db_table = False
                if is_class:
                    is_db_table = issubclass(field, PySqlDatabaseTableInterface)
                if issubclass(type(field), tuple) :
                    str_fields += '{table_alias}.{field_name} {field_alias}, '. \
                        format(table_alias=field[0].get_owner().get_alias(),
                            field_name=field[0].get_db_name(), field_alias=field[1])                    
                elif is_db_table:
                    str_fields += '{table_name}.*, '.format(table_name=field.get_db_name())
                else:                    
                    str_fields += '{alias}.{field_name}, '.format(alias=field.get_owner().get_alias(), \
                        field_name=field.get_db_name())

            str_fields = str_fields[:-2]
        else:
            str_fields = '*'
        
        self.script_fields += ' ' + str_fields + ' '
        
        script = self.script_fields + self.script_from.strip() + self.script_where
        return script.strip()
    
    def values(self, *fields):
        sql = self.get_sql(*fields)
        return self.script_executor.execute_sql_script(sql=sql, params=tuple(self.list_params))



class GenericBaseDmlSelectPostgre(GenricBaseDmlSelect):
    pass

class GenericBaseDmlSelectMySql(GenricBaseDmlSelect):
    pass


class GenericBaseDmlSelectOracle(GenricBaseDmlSelect):
    pass

class GenericBaseDmlSelectSqlServer(GenricBaseDmlSelect):
    pass
