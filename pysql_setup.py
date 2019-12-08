import os, sys, pkgutil, json
from importlib import import_module
from interface import PySqlDatabaseTableInterface
from pysql_class_generator import PySqlClassGenerator

BASE_DIR = os.getcwd()
MODEL_DIR = os.path.join(BASE_DIR, 'models')
MODEL_BACKUP = os.path.join(MODEL_DIR, 'bck')
''' 
 table_structure 
 {
     "table_name": "nome",
     "primary_key": {"name": "nome", "fields": ["id"]},
     "index": {"index_1":{"unique": false, "fields": ["field1", "fiels2"] } },
     "foreign_key": {"fk_1":{"reference": "tbl2"} }
     "fields": [{"name": "na", "type": "VarcharField"}]
 }
'''

def get_list_of_tables():
    list_of_tables = []
    for (_, name, _) in pkgutil.iter_modules([MODEL_DIR]):
        imported_module = import_module('.' + name, 'models')

        classes = list(filter(lambda x: x != 'BaseDbTable' and not x.startswith('__'), 
                        dir(imported_module)))
        for class_name in classes:
            class_model = getattr(imported_module, class_name)

            if issubclass(class_model, PySqlDatabaseTableInterface) and not class_model in list_of_tables:
                list_of_tables.append(class_model)

    return list_of_tables

def get_table_saved_structure(class_to_get):
    table_data =  None
    path = ''
    #get using the actual name.     
    if os.path.exists(os.path.join(MODEL_BACKUP, class_to_get.__name__ + '.json')):
        path = os.path.join(MODEL_BACKUP, class_to_get.__name__ + '.json') 
    elif class_to_get.get_old_class_name() and os.path.exists(os.path.join(MODEL_BACKUP, class_to_get.get_old_class_name() + '.json')):
        path = os.path.exists(os.path.join(MODEL_BACKUP, class_to_get.get_old_class_name() + '.json'))
    
    if path:
        with open(file=path, mode='r') as f:            
            table_data = f.read()
            table_data = json.loads(table_data)
    
    return table_data

def save_table_estructure(class_to_save):
    if os.path.exists(os.path.join(MODEL_BACKUP, class_to_save.__name__ + '.json')):
        os.rename(os.path.join(MODEL_BACKUP, class_to_save.__name__ + '.json'), 
            os.path.join(MODEL_BACKUP, class_to_save.__name__ + '_old.json'))
    
    pk_data = class_to_save.get_script_create_pk()
    data = {"table_name": class_to_save.__name__}
    data["primary_key"] = {}
    data["primary_key"]["name"] = pk_data[0] 
    data["primary_key"]["fields"] = [field.get_db_name() for field in pk_data[2]]

    index_list = class_to_save.get_scripts_indices()
    data["index"] = {}
    for index in index_list:
        data["index"].update({index[0] : {"unique": index[3], "fields": list(index[2])} }) 
    
    fk_list = class_to_save.get_scripts_fk()
    data["foreign_key"] = {}
    for fk in fk_list:
        data["foreign_key"].update({fk[0] : {"reference": ""}}) 
    
    fields = class_to_save.get_fields()
    data["fields"] = []
    for field in fields:    
        data["fields"].append({"name": field.get_db_name(), "type": field.get_generic_type_name()})
    
    if not os.path.exists(MODEL_BACKUP):
        os.mkdir(MODEL_BACKUP)

    with open(file=os.path.join(MODEL_BACKUP, class_to_save.__name__ + '.json'), mode='w', encoding="utf-8") as f:
        f.write(json.dumps(data))

def create_tables(list_of_tables):
    executor = PySqlClassGenerator.get_script_executor()
    table_data = None
    fields = []
    old_fields = []
    for table in list_of_tables:
        table_data = None
        table_data = get_table_saved_structure(table)
        
        if not table_data:
            executor.execute_ddl_script(table.get_script_create_table())
        
        if not table_data or not table_data["primary_key"]:
            executor.execute_ddl_script(table.get_script_create_pk()[1])        
        
        
        if table_data:
            old_fields = table_data["fields"]
            fields = table.get_fields()
            fields_names = [fname.get_db_name() for fname in fields]
            old_field_names = [fname["name"] for fname in old_fields]
            #remove fields
            for field in old_fields:
                if field["name"] not in fields_names:
                    executor.execute_ddl_script(table.get_script_remove_field(field["name"]))
            #add new fields
            for field in fields:
                if field.get_db_name() not in old_field_names:
                    executor.execute_ddl_script(table.get_script_add_field(field))
        
        for index_script in table.get_scripts_indices():
            if not table_data or not index_script[0] in table_data["index"]:
                executor.execute_ddl_script(index_script[1])
        
    
    for table in list_of_tables:
        table_data = None
        table_data = get_table_saved_structure(table)

        for fk_script in table.get_scripts_fk():
            if not table_data or not fk_script[0] in table_data["foreign_key"]:
                executor.execute_ddl_script(fk_script[1])
    
    executor.commit()
    for table in list_of_tables:
        save_table_estructure(table)




def create_database():
    executor = PySqlClassGenerator.get_script_executor()  
    executor.create_database()
    tables = get_list_of_tables()    
    create_tables(list_of_tables=tables)    

def manage_db():
    #check if database is created
      #creates the database if not created
    #check if the cache file exists
      #if exists, then merge files and apply the differences 
      #if not exists, then creates all tables
    
    #fodas vou tentar criar tudo sem verificar nada kkkk
    create_database()
    
methods_store = {
  "MANAGEDB": manage_db  
}

def run():    
    if len(sys.argv) > 1:
        command = sys.argv[1].upper()

        if command in methods_store:
            method = methods_store[command]

            if len(sys.argv) > 1:
                method(*sys.argv[2:])
            else:
                method()
    else:
        raise Exception('Action not defined.')


if __name__ == "__main__":
    run()