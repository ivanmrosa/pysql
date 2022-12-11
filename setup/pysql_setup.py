import os, sys, pkgutil, json, inspect, shutil
from importlib import import_module
try:
    from core.interface import PySqlDatabaseTableInterface
    from core.pysql_class_generator import PySqlClassGenerator
    from core.pysql_command import select, insert
    from setup.MigrationsModels.PySQLStructure import PySQLStructure, PySQLMigration
    from core.pysql_functions import fmax, fcount
    from core.sql_operators import oequ, onin
    from core.unit_of_work import UnitOFWork
except: #fucking problems to integrate in other project
    from pysql.core.interface import PySqlDatabaseTableInterface
    from pysql.core.pysql_class_generator import PySqlClassGenerator
    from pysql.core.pysql_command import select, insert
    from pysql.setup.MigrationsModels.PySQLStructure import PySQLStructure, PySQLMigration
    from pysql.core.unit_of_work import UnitOFWork


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

def insert_first_migrations(model_backup_dir: str):
    if not os.path.exists(model_backup_dir):
        return
    count = select(PySQLStructure).filter(onin(PySQLStructure.ObjectName, 'PySQLStructure', 'PySQLMigration')).\
        values(fcount(alias='count')).get_first()['count']
    
    if count == 0:
        files = os.listdir(model_backup_dir)
        migration = None
        if len(files) > 0:
            migration = create_new_migration('Restoring from local folder.')
        for file in files:
            with open(os.path.join(model_backup_dir, file), 'r') as f:
                structure = f.read()
                table_name = json.loads(structure)['table_name']
                tableStructure = PySQLStructure()
                tableStructure.Migration = migration
                tableStructure.ObjectName = table_name
                tableStructure.Structure = structure
                insert(tableStructure).run()



def create_migration_structure(model_backup_dir : str) -> int:    
    modelsDirectory = os.path.join(os.path.dirname(__file__), 'MigrationsModels')
    tables = get_list_of_tables(modelsDirectory, 'setup.MigrationsModels')
    create_tables(tables, None)
    insert_first_migrations(model_backup_dir)


def create_new_migration(description : str) -> int:
    migration = PySQLMigration()
    migration.Description = description    
    insert(migration).run()
    return select(PySQLMigration).values(fmax(PySQLMigration.Id)).get_first()["Id"]

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

def get_table_saved_structure(class_to_get):
        
    table_data =  None

    try:
        migration = select(PySQLStructure).filter(oequ(PySQLStructure.ObjectName, class_to_get.__name__)).\
            values(fmax(PySQLStructure.Id, 'id')).get_first()
        
        if not migration['id']:
            migration = select(PySQLStructure).filter(oequ(PySQLStructure.ObjectName, class_to_get.get_old_class_name())).\
                values(fmax(PySQLStructure.Id, 'id')).get_first()
            
        if not migration['id']:
            return None
        
        table_data = json.loads(select(PySQLStructure).\
            filter(oequ(PySQLStructure.Id, migration['id'])).\
            values(PySQLStructure.Structure).get_first()['Structure'])
    except:
        if class_to_get.__name__ in ('PySQLStructure', 'PySQLMigration'):
            return None
        raise

    return table_data

def save_table_structure(class_to_save, migration_id):   
    pk_data = class_to_save.get_script_create_pk()
    data = {"table_name": class_to_save.__name__, "db_table_name": class_to_save.get_db_name()}
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
    
    structure = PySQLStructure()
    structure.Migration = migration_id
    structure.ObjectName = class_to_save.__name__
    structure.Structure = json.dumps(data)
    insert(structure).run()

    
def create_tables(list_of_tables, migration_id):
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
            executor.execute_ddl_script(table.get_script_create_pk(False)[1])    
        
        if not table_data:
            many_to_many_fields = table.get_many_to_many_fields()
            fks = []
            for field in many_to_many_fields:
                executor.execute_ddl_script(field.get_script())
                executor.execute_ddl_script(field.get_middle_class().get_script_create_pk(False)[1]) 
                fks = field.get_middle_class().get_scripts_fk(False)
                for fk in fks:                    
                    executor.execute_ddl_script(fk[1])
                
                indexes = field.get_middle_class().get_scripts_indices(False)
                for index in indexes:
                    executor.execute_ddl_script(index[1])
        
                
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
        
        if table.__name__ in ('PySQLStructure', 'PySQLMigration'):  
            executor.commit()
    
    for table in list_of_tables:
        table_data = None
        table_data = get_table_saved_structure(table)
                                
        for fk_script in table.get_scripts_fk(False):
            if not table_data or not fk_script[0] in table_data["foreign_key"]:                
                executor.execute_ddl_script(fk_script[1])
        
    
    executor.commit()
    if not migration_id:
        migration_id = create_new_migration('Initial creation.')
    for table in list_of_tables:
        save_table_structure(table, migration_id)


def create_database(models_directory, package, description):
    try:
        executor = PySqlClassGenerator.get_script_executor()  
        executor.create_database()
        print('database created.')
        model_backup_dir = os.path.join(models_directory, 'bck')     
        create_migration_structure(model_backup_dir)
        migrationId = create_new_migration(description)
        tables = get_list_of_tables(models_directory, package)       
        create_tables(list_of_tables=tables, migration_id=migrationId)  
        print('process concluded.')   
        UnitOFWork.save()
    except:
        UnitOFWork.discard()
        raise

def drop_database(ask_question='y'):
    yes_no = 'y'
    if ask_question == 'y':
        yes_no = input('Do you really want to drop the database? All data will be lost. If yes, we really recommend a backup. y/n ')
    if yes_no == 'y': 
        executor = PySqlClassGenerator.get_script_executor()  
        executor.close_connection()
        executor.drop_database()
        print('database dropped.')

def clear_cache(ask, model_backup_directory):
    if ask == 'y':
        yes_no = input('Do you really want to clear the cache? All scripts will be runned again and errors may occur. If yes, we really recommend a backup. y/n ')
    else:
        yes_no = 'y'

    if yes_no == 'y': 
        if os.path.exists(model_backup_directory):
            shutil.rmtree(model_backup_directory)
        print('Cache removed.')

def manage_db(clear_cache_param='', ask_question='y', base_dir="", models_package="models", description=""):    
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

    if clear_cache_param == 'RECREATEDB':
        clear_cache(ask=ask_question, model_backup_directory=MODEL_BACKUP)
        drop_database(ask_question)

    create_database(MODEL_DIR, models_package, description)



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