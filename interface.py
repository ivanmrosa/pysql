from abc import ABC

class PySqlDatabaseTableInterface(ABC):
    def get_alias(self):
        pass
    def set_alias(self):
        pass

class PySqlOperatorsInterface(ABC):
    def get_operator(self):
        pass
    
    def get_sql_text(self, value_as_paremeter = False, list_of_parameters = []):
        pass


class PySqlFieldInterface(ABC):
    
    def get_db_name(self):
        pass
    
    def get_script(self):
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
    
class PySqlCommandInterface(ABC):
    pass
    
    
#Bolab.join(Dbol, (Bolab.cod_bol,  Dbol.cod_bol)).\
#    filter( ftor((eq(dbol.tipo, 3), eq(dbol.codigo, 23)), (eq(dbol.tipo, 3), eq(dbol.codigo, 23))) ).\
#    select()        


#select *
# from im_bolab
#join im_dbol 
#  on im_bolab.cod_bol = im_dbol.cod_bol
#where ( (im_dbol.tipo = 3 and codigo = 23) or (im_dbol.tipo = 2 and codigo = 24) )