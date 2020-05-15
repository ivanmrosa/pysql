import os, sys, pkgutil, json, inspect, shutil
from importlib import import_module
try:
    from core.interface import PySqlDatabaseTableInterface
    from core.pysql_class_generator import PySqlClassGenerator
except: #fucking problems to integrate in other project
    from pysql.core.interface import PySqlDatabaseTableInterface
    from pysql.core.pysql_class_generator import PySqlClassGenerator


#BASE_DIR = os.getcwd()
#MODEL_DIR = os.path.join(BASE_DIR, 'models')
#MODEL_BACKUP = os.path.join(MODEL_DIR, 'bck')
''' 
 table_structure 
 {
     "table_name": "nome",
     "primary_key": {"name": "nome", "fields": ["id"]},
     "index": {"index_1":{"unique": false, "fields": ["field1", "fiels2"] } },
     "foreign_key": {"fk_1":{"reference": "tbl2"} }
     "fields": [{"name": "na", "type": "VarcharField"}],
     "check_constraints": {"name_check": "script"}
 }
'''

def get_list_of_tables(models_directory, models_package):           
    list_of_tables = []
    iter_modules = pkgutil.iter_modules([models_directory])    

    for (_, name, _) in iter_modules:
        imported_module = import_module(models_package + '.' + name, None)

        classes = list(filter(lambda x: x != 'BaseDbTable' and not x.startswith('__'), 
                        dir(imported_module)))
        try:
            for class_name in classes:
                class_model = getattr(imported_module, class_name)

                if inspect.isclass(class_model) and issubclass(class_model, PySqlDatabaseTableInterface) and not class_model in list_of_tables:
                    if not ('Abstract' in class_model.__dict__ and getattr(class_model, 'Abstract') == True):
                        list_of_tables.append(class_model)
        except:
            print(class_model)
            raise 

    return list_of_tables

def get_table_saved_structure(class_to_get, model_backup_directory):
    table_data =  None
    path = ''
    #get using the actual name.     
    if os.path.exists(os.path.join(model_backup_directory, class_to_get.__name__ + '.json')):
        path = os.path.join(model_backup_directory, class_to_get.__name__ + '.json') 
    elif class_to_get.get_old_class_name() and os.path.exists(os.path.join(model_backup_directory, class_to_get.get_old_class_name() + '.json')):
        path = os.path.exists(os.path.join(model_backup_directory, class_to_get.get_old_class_name() + '.json'))
    
    if path:
        with open(file=path, mode='r') as f:            
            table_data = f.read()
            table_data = json.loads(table_data)
    
    return table_data

def save_table_estructure(class_to_save, model_backup_directory):
    if os.path.exists(os.path.join(model_backup_directory, class_to_save.__name__ + '.json')):
        os.rename(os.path.join(model_backup_directory, class_to_save.__name__ + '.json'), 
            os.path.join(model_backup_directory, class_to_save.__name__ + '_old.json'))
    
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
    
    checks = class_to_save.get_scripts_check_constraints()
    data["check_constraints"] = {}
    for check in checks:
        data["check_constraints"].update({check[0] : check[1]})
    
    if not os.path.exists(model_backup_directory):
        os.mkdir(model_backup_directory)

    with open(file=os.path.join(model_backup_directory, class_to_save.__name__ + '.json'), mode='w', encoding="utf-8") as f:
        f.write(json.dumps(data))

def create_tables(list_of_tables, model_backup_directory):
    executor = PySqlClassGenerator.get_script_executor()
    table_data = None
    fields = []
    old_fields = []
    for table in list_of_tables:
        table_data = None
        table_data = get_table_saved_structure(table, model_backup_directory)
        
        if not table_data:            
            executor.execute_ddl_script(table.get_script_create_table())
        
        if not table_data or not table_data["primary_key"]:            
            executor.execute_ddl_script(table.get_script_create_pk(False)[1])        
        
        
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
        
        for index_script in table.get_scripts_indices(False):
            if not table_data or not index_script[0] in table_data["index"]:
                executor.execute_ddl_script(index_script[1])
        
        for check_script in table.get_scripts_check_constraints(False):
            if not table_data or not check_script[0] in table_data["check_constraints"]:
                executor.execute_ddl_script(check_script[1])
    
    for table in list_of_tables:
        table_data = None
        table_data = get_table_saved_structure(table, model_backup_directory)
        
        
        for fk_script in table.get_scripts_fk(False):
            if not table_data or not fk_script[0] in table_data["foreign_key"]:                
                executor.execute_ddl_script(fk_script[1])
    
    executor.commit()
    for table in list_of_tables:
        save_table_estructure(table, model_backup_directory)


def create_database(models_directory, model_backup_directory, package):
    executor = PySqlClassGenerator.get_script_executor()  
    executor.create_database()
    print('database created.')
    tables = get_list_of_tables(models_directory, package)       
    create_tables(list_of_tables=tables, model_backup_directory=model_backup_directory)  
    print('process concluded.')   

def drop_database(ask_question='y'):
    yes_no = 'y'
    if ask_question == 'y':
        yes_no = input('Do you really want to drop the database? All data will be lost. If yes, we really recommend a backup. y/n ')
    if yes_no == 'y': 
        executor = PySqlClassGenerator.get_script_executor()  
        executor.close_connection()
        executor.drop_database()
        print('database droped.')

def clear_cache(ask, model_backup_directory):
    if ask == 'y':
        yes_no = input('Do you really want to clear the cache? All scripts will be runned again and errors may occur. If yes, we really recommend a backup. y/n ')
    else:
        yes_no = 'y'

    if yes_no == 'y': 
        if os.path.exists(model_backup_directory):
            shutil.rmtree(model_backup_directory)
        print('Cache removed.')

def manage_db(clear_cache_param='', ask_question='y', base_dir="", models_package="models"):    
    #check if database is created
      #creates the database if not created
    #check if the cache file exists
      #if exists, then merge files and apply the differences 
      #if not exists, then creates all tables
        
    if base_dir:
        if os.path.exists(base_dir):
            BASE_DIR = base_dir
        else:
            BASE_DIR = os.path.join(os.getcwd(), base_dir)
    else:
        BASE_DIR = os.getcwd()
    
    print(BASE_DIR)
    
    MODEL_DIR = os.path.join(BASE_DIR, 'models')
    MODEL_BACKUP = os.path.join(MODEL_DIR, 'bck')     


    if clear_cache_param.upper() == 'CLEARCACHE': 
        clear_cache(True, MODEL_BACKUP)
    elif clear_cache_param == 'RECREATEDB':
        clear_cache(False, MODEL_BACKUP)
        drop_database(ask_question)

    create_database(MODEL_DIR, MODEL_BACKUP, models_package)



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