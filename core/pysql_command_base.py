from . field_tools import FieldTools
#from sql_db_tables import BaseDbTable 
from . interface import PySqlDatabaseTableInterface, PySqlCommandInterface, PySqlFieldInterface, PySqlDistinctClause
import inspect
from . friendly_data import FriendlyData


#from pysql_command import update
#from sql_operators import oequ


class DmlBase(PySqlCommandInterface):    
    def __init__(self, script_executor_object):
        self.script_executor = script_executor_object        

    def get_param_representation(self):
        return '%s'

class GenricBaseDmlScripts(DmlBase):
    def __init__(self, script_executor_object):
        super(GenricBaseDmlScripts, self).__init__(script_executor_object)
        self.list_operations = []
        self.list_params = []
        self.script_fields = ''
        self.script_from = ''
        self.script_where = ''
                
    def add_operation(self, type, table):
        self.list_operations.append({"type": type, "table" : table})
        
    def add_base_join(self, table):
        self.script_from += ' JOIN {table} {alias} ON '.format(table=table.get_db_name(), alias=table.get_alias())
            
    def add_script_used_for_join_filter(self, script, first_filter):
        self.script_from += script

    def add_base_filter_join_using_fk(self, table, table_related, field, field_related, first_filter):
        sql_join_filter = ''
        if first_filter:
            temp_script = '{alias_prior}.{field_name_prior} = {alias}.{field_name} '
        else:
            temp_script = 'AND {alias_prior}.{field_name_prior} = {alias}.{field_name} '
        
        sql_join_filter += temp_script.format(alias_prior=table_related.get_alias(), 
            field_name_prior=field_related.get_db_name(),
            alias=table.get_alias(), field_name=field.get_db_name())
        
        self.add_script_used_for_join_filter(sql_join_filter, first_filter)
        
    def end_operation_add_join(self, table):
        self.script_from = self.script_from.strip()
        self.add_operation('JOIN', table)
   
    
    def join(self, table, table_to_relate=None, tuple_fields_comparations = ()):        
        old_from_script = self.script_from
        self.add_base_join(table)
        if len(tuple_fields_comparations) == 0:
            
            if len(self.list_operations) > 0:                
                
                prior_table = table_to_relate if table_to_relate else self.list_operations[-1:][0]["table"]
                fk_fields = table.get_fk_fields()

                count = 0
                for fk_field in fk_fields:
                    if issubclass(fk_field.get_related_to_class(), prior_table):
                        self.add_base_filter_join_using_fk(table, prior_table, fk_field, 
                            fk_field.get_related_to_class().get_pk_fields()[0], count==0)
                        count += 1
                
                if count == 0:
                    fk_fields = prior_table.get_fk_fields()
                    for fk_field in fk_fields:
                        if issubclass(fk_field.get_related_to_class(), table):
                            self.add_base_filter_join_using_fk(table, prior_table, 
                                fk_field.get_related_to_class().get_pk_fields()[0], fk_field, count==0)
                            count += 1

                if count == 0:
                    self.script_from = old_from_script
                    fk_fields = table.get_many_to_many_fields()
                    for fk_field in fk_fields:
                        if issubclass(fk_field.get_related_to_class(), table):
                            #self.join(table, fk_field.get_middle_class())
                            self.join(fk_field.get_middle_class())                            
                            self.join(fk_field.get_related_to_class())
                            count += 1
                    
                    if count == 0:
                        fk_fields = prior_table.get_many_to_many_fields()
                        for fk_field in fk_fields:
                            if issubclass(fk_field.get_related_to_class(), table):
                                self.join(fk_field.get_middle_class())                            
                                self.join(fk_field.get_related_to_class())

                                count += 1
                              
            else:
                raise Exception('Join must be preceded for an operator update or select')
        else:
            raise NotImplementedError
        
        self.end_operation_add_join(table)

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
                    
        self.script_where += '(' + temporary_script + ')'

        return self


class GenricBaseDmlSelect(GenricBaseDmlScripts):
    def __init__(self, script_executor_object):
        super(GenricBaseDmlSelect, self).__init__(script_executor_object)
        self.fields_to_select = None
        self.script_order_by = ''
        self.aggregated_fields = []
        self.all_fields = []

    def set_fields(self, *fields):
        self.fields_to_select = fields
        return self
        
    
    def get_fields_names_and_script(self, *fields):
        fields_names = []   
        str_fields = ''

        if fields:
            if len(fields) == 1 and issubclass(type(fields[0]), PySqlDistinctClause):
                self.fields_to_select = fields[0].get_fields()
                str_fields += ' DISTINCT '
            else:
                self.fields_to_select = fields
        
        if self.fields_to_select:
            for field in self.fields_to_select:
                #tuple means field with alias (Table.field, 'field_alias')
                is_class = inspect.isclass(field)
                is_db_table = False
                if is_class:
                    is_db_table = issubclass(field, PySqlDatabaseTableInterface)
                if issubclass(type(field), tuple) :
                    fields_names.append(field[1])
                    self.all_fields.append(field[0])
                    if field[0].is_used_in_aggregated_function():
                        self.aggregated_fields.append(field[0])
                    str_fields += field[0].get_sql_for_field(use_alias = False) + ' ' + field[1] + ', '
                elif is_db_table:                                        
                    str_fields += '{table_name}.*, '.format(table_name=field.get_alias())
                    for f in field.get_fields():
                        fields_names.append(f.get_db_name())
                        self.all_fields.append(field)
                else:                    
                    fields_names.append(field.get_alias())
                    self.all_fields.append(field)
                    if field.is_used_in_aggregated_function():
                        self.aggregated_fields.append(field)

                    str_fields += field.get_sql_for_field() + ', '

            str_fields = str_fields[:-2]
        else:            
            for operation in self.list_operations:                
                table = operation["table"]
                for f in table.get_fields():
                    str_fields += f.get_sql_for_field() + ', '
                    fields_names.append(f.get_db_name())
                    self.all_fields.append(f)
            
            str_fields = str_fields[:-2]
                
        return fields_names , str_fields   

    def get_script_group_by(self):
        str_group = ''
        if len(self.aggregated_fields) > 0:
            
            for f in self.all_fields:
                if not f.is_used_in_aggregated_function():
                    str_group += f.get_sql_for_field(use_alias=False) + ', '
            if str_group:
               str_group = ' GROUP BY ' + str_group[:-2]
                
        return str_group

    def get_sql_and_fields_names(self, *fields):                
        fields_names, str_fields = self.get_fields_names_and_script(*fields)
        script = self.script_fields + ' ' + str_fields + ' ' +  self.script_from.strip() + self.script_where + self.get_script_group_by() + self.script_order_by
        return script.strip(), fields_names
    
    def get_sql(self, *fields):
        sql, fields_names = self.get_sql_and_fields_names(*fields)
        return sql
    
    def order_by(self, *fields):
        for f in fields:            
            self.script_order_by += f.get_sql_for_field(use_alias=False) + ', '
            if not f in self.all_fields:
                self.all_fields.append(f)
        
        self.script_order_by = ' ORDER BY ' + self.script_order_by[:-2]

        return self

    def values(self, *fields):
        sql, fields_names = self.get_sql_and_fields_names(*fields)
        
        simple_data = self.script_executor.execute_select_script(sql=sql, params=tuple(self.list_params))
        friendly_data = FriendlyData(simple_data,  fields_names)
        return friendly_data


class GenericBaseDmlInsertUpdate(DmlBase):
    def __init__(self, script_executor_object):
        super(GenericBaseDmlInsertUpdate, self).__init__(script_executor_object)        
        self.table = None 
        self.params = ()

    def commit(self):
        if self.script_executor:
            self.script_executor.commit()
            self.script_executor.close_connector()

    def run(self, commit=True):        
        self.script_executor.execute_dml_script(self.get_script(), self.params, commit)
    
    def get_script(self):
        return ''

class GenericBaseDmlInsert(GenericBaseDmlInsertUpdate):
    select_executor = None
    oequ_clause = None
    max_func = None
    _many_to_many_insert = ()

    def get_script(self):
        self._many_to_many_insert = ()
        if issubclass(self.table, PySqlDatabaseTableInterface):                        
            self.params = ()        

            script_fields = 'INSERT INTO ' + self.table.get_db_name() + '('
            script_values = 'VALUES('
            fields = self.table.get_fields() 
            for field in fields:
                if not field.is_many_to_many():
                    if (field.value != None):
                        self.params += (field.value, )
                        script_fields += field.get_db_name() + ', '
                        script_values += '{param}, '.format(param=self.get_param_representation())
                else:
                    self._many_to_many_insert +=  ((field.get_middle_class(), field.value),)

            script_fields = script_fields[:-2] + ') '
            script_values = script_values[:-2] + ')'
            return  script_fields + script_values
        else:
            return ''
    
    def run(self, commit=True):        
        self.script_executor.execute_dml_script(self.get_script(), self.params, commit)
        
        if len(self._many_to_many_insert) > 0:
            id = None
            where = ()
            fields = None
            if self.select_executor:
                fields = self.table.get_fields()
                for field in fields:
                    if not field.is_many_to_many():
                        if field.value != None:
                            where += (self.oequ_clause(field, field.value), )
                id = self.select_executor(self.table).filter(*where).values(self.max_func(self.table.get_pk_fields()[0]))[0][0]

                for to_ins in self._many_to_many_insert:
                    self.table = to_ins[0]
                    self.table.clear()
                    self.table.get_fields()[1].value = id
                    for val in to_ins[1]:
                        self.table.get_fields()[2].value = val
                        self.run()


class GenericBaseDmlUpdateDelete(GenricBaseDmlScripts, GenericBaseDmlInsertUpdate):
    def add_base_join(self, table):
        if self.script_from:
            self.script_from += ', {table} {alias} '.format(table=table.get_db_name(), alias=table.get_alias())
        else:
            self.script_from += ' FROM {table} {alias} '.format(table=table.get_db_name(), alias=table.get_alias())
    
    def add_script_used_for_join_filter(self, script, first_filter):
        if self.script_where:
            if first_filter:
                self.script_where += ' AND ' + script
            else:
                self.script_where += script
        else:
            self.script_where += ' WHERE ' + script
   
    def run(self, commit=True):   
        script = self.get_script()     
        self.params += tuple(self.list_params)
        self.script_executor.execute_dml_script(script, self.params, commit)    


class GenericBaseDmlUpdate(GenericBaseDmlUpdateDelete):

    def get_script(self):
        script = ''
        if issubclass(self.table, PySqlDatabaseTableInterface): 
            script = 'UPDATE {table} {sql_set} {sql_from} {sql_where}'.format(table=self.table.get_db_name(), sql_set=self.get_sql_set(),
               sql_from=self.script_from, sql_where=self.script_where)       
            return script
        else:
            Exception('The update parameters must be a PySqlDataBaseTAbleInterface')
    
    def get_filled_fields(self):
        fields = self.table.get_fields()
        filled_fields = []
        for field in fields:
            if field.value != None:
                filled_fields.append(field)
        return filled_fields

    def get_sql_set(self):
        sql_field = ' SET '
        value = None
        fields = self.get_filled_fields()
        for field in fields:
            if FieldTools.is_db_field(field.value):
                value = field.get_owner().get_alias() + '.' + field.value.get_db_name()
            else:
                value = '{param}'.format(param=self.get_param_representation())
                self.params += (field.value,)
            
            sql_field += field.get_db_name() + ' = ' + value + ', '  
        
        return sql_field[:-2]
            


class GenericBaseDmDelete(GenericBaseDmlUpdateDelete):
        
    def get_script(self):
        script = ''
        if issubclass(self.table, PySqlDatabaseTableInterface): 
            if self.script_from:
                script = 'DELETE FROM {table} WHERE EXISTS ( SELECT ''1'' {sql_from} {sql_where} )'.format(table=self.table.get_db_name(), 
                sql_from=self.script_from, sql_where=self.script_where)       
            else:
                script = 'DELETE FROM {table} {sql_from} {sql_where}'.format(table=self.table.get_db_name(), sql_from=self.script_from, 
                sql_where=self.script_where)       
            return script
        else:
            Exception('The update parameters must be a PySqlDataBaseTAbleInterface')


#sql
class GenericBaseDmlSelectPostgre(GenricBaseDmlSelect):
    pass

class GenericBaseDmlSelectMySql(GenricBaseDmlSelect):
    pass


class GenericBaseDmlSelectOracle(GenricBaseDmlSelect):
    pass

class GenericBaseDmlSelectSqlServer(GenricBaseDmlSelect):
    pass

class GenericBaseDmlSelectSqlite(GenricBaseDmlSelect):
    def get_param_representation(self):
        return '?'


#insert
class GenericBaseDmlInsertPostgre(GenericBaseDmlInsert):
    pass

class GenericBaseDmlInsertMySql(GenericBaseDmlInsert):
    pass


class GenericBaseDmlInsertOracle(GenericBaseDmlInsert):
    pass

class GenericBaseDmlInsertSqlServer(GenericBaseDmlInsert):
    pass

class GenericBaseDmlInsertSqlite(GenericBaseDmlInsert):
    def get_param_representation(self):
        return '?'


#update
class GenericBaseDmlUpdatePostgre(GenericBaseDmlUpdate):
    pass

class GenericBaseDmlUpdateMySql(GenericBaseDmlUpdate):
    pass

class GenericBaseDmlUpdateOracle(GenericBaseDmlUpdate):
    pass

class GenericBaseDmlUpdateSqlServer(GenericBaseDmlUpdate):
    pass

class GenericBaseDmlUpdateSqlite(GenericBaseDmlUpdate):
    execute_using_sql = False
    def get_param_representation(self):
        return '?'
    
    def get_sql_fiels_to_update(self, include_pk=False):
        fields = self.get_filled_fields()
        sql_field = ''
        for field in fields:
            if inspect.isclass(field.value) and issubclass(field.value, PySqlFieldInterface):
                value = field.get_owner().get_alias() + '.' + field.get_alias()
            else:
                value = '{param}'.format(param=self.get_param_representation())
                self.params += (field.value,)
            
            sql_field +=  value +  ' ' + field.get_db_name() + ', '  
        
        pks = self.table.get_pk_fields()
        if len(pks) > 1:
            Exception('Table ' + self.table.get_db_name() + ' has a composite primary key. It`s not allowed for updates using from.')
        
        if include_pk:
            sql_field += self.table.get_alias() + '.' + pks[0].get_db_name() + ' f_primay_key, '  
        
        return sql_field[:-2]


    def get_script(self):
        script = ''
        if issubclass(self.table, PySqlDatabaseTableInterface): 
            if self.script_from:
                self.execute_using_sql = True
                self.add_base_join(self.table)                
                script = 'SELECT {fields} {sql_from} {sql_where}'.format(
                    fields=self.get_sql_fiels_to_update(include_pk=True), sql_from=self.script_from, sql_where=self.script_where)
                return script
            else:      
                self.execute_using_sql = False
                script = 'UPDATE {table} {sql_set} {sql_from} {sql_where}'.format(table=self.table.get_db_name(), sql_set=self.get_sql_set(),
                sql_from=self.script_from, sql_where=self.script_where)                  
            return script
        else:
            Exception('The update parameters must be an PySqlDataBaseTAbleInterface')
    

    def run(self, commit=True):   
        script = self.get_script()     
        self.params += tuple(self.list_params)        
        if self.execute_using_sql:            
            data = self.script_executor.execute_select_script(script, self.params)
            print(self.params)
            print(data)                        

            fields = self.get_filled_fields()
            len_fields = len(fields)          
            index = 0
            update_filter = ()
                                    
            for row in data:
                self.table.clear()
                for ro in row:
                    if index < len_fields:                                                
                        fields[index].value = ro
                    elif index == len_fields:
                        update_filter = (ro, )
                    index += 1
                index = 0
                if not update_filter:
                    Exception('No filter param primary found')
                self.params = ()
                update_script = 'update {table} {fields} where {pk_field_name} = ?'.format(table=self.table.get_db_name(), fields=self.get_sql_set(),
                    pk_field_name=self.table.get_pk_fields()[0].get_db_name())                                
                self.params +=  update_filter 
                self.script_executor.execute_dml_script(update_script, self.params, False)
            
            if commit:
                self.script_executor.commit()    
            
        else:
            self.script_executor.execute_dml_script(script, self.params, commit)    


#delete GenericBaseDmDelete
class GenericBaseDmlDeletePostgre(GenericBaseDmDelete):
    pass

class GenericBaseDmlDeleteMySql(GenericBaseDmDelete):
    pass

class GenericBaseDmlDeleteOracle(GenericBaseDmDelete):
    pass

class GenericBaseDmlDeleteSqlServer(GenericBaseDmDelete):
    pass

class GenericBaseDmlDeleteSqlite(GenericBaseDmDelete):
    pass