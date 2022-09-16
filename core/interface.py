from abc import ABC

class PySqlFieldInterface(ABC):
    _property_name = ''
    _order = 0
    def get_db_name(self):
        pass
    
    def get_script(self):
        pass

    def set_owner(self, owner):
        pass
   
    def set_db_name(self, db_name):
        pass

class MetaDbTable(type):    
    def __init__(self, name, bases, attr_dict):        
        super().__init__(name , bases , attr_dict)
        setattr(self, "class_name", name)
        order = 0
        for key, attr in attr_dict.items():            
            if isinstance(attr, PySqlFieldInterface):
                attr.set_owner(self)
                attr._property_name = key
                attr._order = order
                if not attr.get_db_name():
                    attr.set_db_name(key)
                order += 1
                                                            
class PySqlDatabaseTableInterface(metaclass=MetaDbTable):    
   
    def get_alias(cls):
        pass

    def set_alias(cls, alias):
        pass
    
    def get_db_name(cls):
        pass
        
    def get_copy(cls, alias):
        pass        
    
    def get_fields(cls):        
        pass        

    def get_field_by_db_name(cls, name):
        pass
    
    def get_script_create_table(cls):
        pass    
    
    def get_script_drop_table(cls, table_name=''):
        pass

    def get_script_create_pk(cls, creating_table=False):
        pass                        
        
    def get_pk_fields(cls):
        pass
    
    def get_scripts_indices(cls, creating_table=False):
        pass

    def get_fk_fields(cls):
        pass    
        
    def get_many_to_many_fields(cls):
        pass
    
    def get_scripts_fk(cls, creating_table=False):
        pass

    def get_scripts_check_constraints(cls, creating_table=False):
        pass
    
    def get_script_remove_field(cls, db_field_name):                    
        pass
    
    def get_script_add_field(cls, field_class):
        pass

    #@classmethod
    #def get_script_drop_table(cls):
    #    return 'DROP TABLE ' + cls.get_db_name()
    
    def compound_indexes_list(cls):
        pass    
    
    def get_class_name(cls):
        pass
    
    
    def get_old_class_name(cls):
        pass
        
    def get_old_db_name(cls):
        pass
    
    def clear(cls):
        pass
class PySqlOperatorsInterface(ABC):
    def get_operator(self):
        pass
    
    def get_sql_text(self, value_as_paremeter = False, list_of_parameters = []):
        pass


class PySqlRunScriptInterface(ABC):
    
    def open_connection(self):
        pass
    
    def close_connection(self):
        pass
    
    def execute_ddl_script(self, script):
        pass
    
    def execute_dml_script(self, script, params, auto_commit = False):
        pass

    def execute_select_script(self, sql, params):
        pass

    def create_database(self):        
        pass
    
    def drop_database(self):
        pass
    
    def commit(self):
        pass
    
    def rollback(self):
        pass
    
class PySqlCommandInterface(ABC):
    pass
    
class PySqlDistinctClause(ABC):
    def get_fields(self):
        pass


#Bolab.join(Dbol, (Bolab.cod_bol,  Dbol.cod_bol)).\
#    filter( ftor((eq(dbol.tipo, 3), eq(dbol.codigo, 23)), (eq(dbol.tipo, 3), eq(dbol.codigo, 23))) ).\
#    select()        


#select *
# from im_bolab
#join im_dbol 
#  on im_bolab.cod_bol = im_dbol.cod_bol
#where ( (im_dbol.tipo = 3 and codigo = 23) or (im_dbol.tipo = 2 and codigo = 24) )