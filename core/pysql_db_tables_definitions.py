from . interface import PySqlDatabaseTableInterface
from . interface import PySqlFieldInterface
#from db_types import  
#from db_types import ForeignKey
import inspect


class GenericDbTable(PySqlDatabaseTableInterface):
    
    __alias = None
    __db_name = None
    __pk_fields = ()
    __compound_indexes = []
    
    def __init__(self) -> None:
        super().__init__()
        self.clear()

    @classmethod
    def get_alias(cls):
        if not cls.__alias:
            cls.set_alias(cls.get_db_name())
        
        return cls.__alias

    @classmethod
    def set_alias(cls, alias):
        if alias != "BaseDbTable":
            cls.__alias = alias

    @classmethod
    def get_db_name(cls):
        if not cls.__db_name and cls.class_name != "BaseDbTable":
            cls.__db_name = cls.class_name
        return cls.__db_name

    @classmethod
    def get_copy(cls, alias) -> PySqlDatabaseTableInterface:
        copy = type(alias, (cls, ), {})
        copy.set_alias(alias)
        copy.db_name = cls.get_db_name()        
        return copy
        
    @classmethod
    def get_fields(cls):        

        attributes =  inspect.getmembers(cls)
        fields = []
        for attr in attributes:
            name = attr[0]
            item = attr[1]
            if not inspect.isroutine(item):
                if not(name.startswith('__') and name.endswith('__')) and getattr(item, '_Field__is_db_field', False):
                    fields.append(item)
        fields.sort(key=lambda field : field._order)
        return  fields
        

    @classmethod    
    def get_field_by_db_name(cls, name):
        fields = cls.get_fields()
        for field in fields:
            if field.get_db_name() == name:
                return field
        
        return None
    
    @classmethod
    def get_script_create_table(cls):
        script_create = 'CREATE TABLE ' + cls.get_db_name() + '('
        fields = cls.get_fields()
        
        for field in fields:
            if not field.is_many_to_many():
                script_create += field.get_script() + ', '
        
        script_create = script_create[:-2]
        script_create += ')'
        return script_create
    
    @classmethod
    def get_script_drop_table(cls, table_name=''):
        tbl = table_name
        if not tbl:
            tbl = cls.get_db_name()
        return 'DROP TABLE ' + tbl

    @classmethod
    def get_script_create_pk(cls, creating_table=False):
        fields = cls.get_pk_fields()
        name = ""
        if len(fields) > 0:
            name = 'PK_' + cls.get_db_name()
            script = 'ALTER TABLE ' + cls.get_db_name() + ' ADD CONSTRAINT ' + name + \
                ' PRIMARY KEY('
            
            for field in fields:
                script += field.get_db_name() + ', '
        
            script = script[:-2] + ')'
            return (name, script, fields)
        else:
            return ''
                        
    @classmethod
    def get_pk_fields(cls):
        if len(cls.__pk_fields) == 0:
            fields = cls.get_fields()
            for field in fields:
                if field.is_primary_key():
                    cls.__pk_fields += (field,)
        
        return cls.__pk_fields

    @classmethod
    def get_scripts_indices(cls, creating_table=False):
        fields = cls.get_fields()
        name = ''        
        indices = ()
        for field in fields:
            if field.has_unique_index():
                name = 'UX_' + cls.get_db_name() + '_' + field.get_db_name()
                indices += ((name, 'CREATE UNIQUE INDEX ' + name + \
                    ' ON ' + cls.get_db_name() + '('+ field.get_db_name() + ')', (field.get_db_name(), ), True ), )
            elif field.has_normal_index():
                name = 'IX_' + cls.get_db_name() + '_' + field.get_db_name()
                indices += ((name, 'CREATE INDEX ' + name + \
                    ' ON ' + cls.get_db_name() + '('+ field.get_db_name() + ')', (field.get_db_name(), ), False ), )               
        
        for index in cls.compound_indexes_list():
            unique = index[1]
            name = index[0]
            compound_fields = index[2]
            fields_names = ''            
            
            for field_name in compound_fields:
                fields_names += field_name.get_db_name() + ','
            
            fields_names = fields_names[:-1]   

            str_unique = ''
            if unique == True:
                str_unique = ' UNIQUE'
            
            indices += ((name, 'CREATE' + str_unique + ' INDEX ' + name + ' ON ' + cls.get_db_name() + '(' + fields_names + ')', tuple([fname.get_db_name() for fname in compound_fields]), unique ),)

        return indices
    
    @classmethod
    def get_fk_fields(cls):
        fields = cls.get_fields()        
        fks = ()
        for field in fields:
            if field.is_foreign_key():
                fks += (field, )
        return fks
    
    @classmethod
    def get_many_to_many_fields(cls):
        fields = cls.get_fields()        
        fks = ()
        for field in fields:
            if field.is_many_to_many():
                fks += (field, )
        return fks

    @classmethod
    def get_scripts_fk(cls, creating_table=False):
        fields = cls.get_fk_fields()        
        fks = ()
        name = ""
        for field in fields:
            related_pk_fields = field.get_related_to_class().get_pk_fields()
            if len(related_pk_fields) == 0:
                raise Exception(field.get_related_to_class().get_db_name() + ' has not a primary key.')
            elif len(related_pk_fields) > 1:
                raise Exception(field.get_related_to_class().get_db_name() + ' has a composite primary key')
            name = 'FK_' + cls.get_db_name()[0:3] + \
                '_' + field.get_related_to_class().get_db_name()[0:3] + '_' + field.get_db_name() 
            fks += ((name, 'ALTER TABLE ' + cls.get_db_name() + ' ADD CONSTRAINT '+ name +
                ' FOREIGN KEY(' +  field.get_db_name() + ')' + 
                ' REFERENCES ' + field.get_related_to_class().get_db_name() + 
                '(' +  related_pk_fields[0].get_db_name() + ')' + field.get_script_cascade(),  
                field.get_related_to_class().get_db_name()),)
        return fks
    
    @classmethod
    def get_scripts_check_constraints(cls, creating_table=False):
        fields = cls.get_fields()
        check = None
        checks = ()
        name = ''
        for field in fields:
            check = field.get_check_constraint_validation()
            if check:
                name = 'CK_' + field.get_owner().get_db_name() + '_' + field.get_db_name() 
                checks += ((name, 'ALTER TABLE ' + field.get_owner().get_db_name() + ' ADD CONSTRAINT '+ name +' CHECK (' + check  + ')'), )
        
        return checks

    @classmethod
    def get_script_remove_field(cls, db_field_name):                    
        field = cls.get_field_by_db_name(db_field_name)
        if field:
            if field.is_many_to_many_filed():
                return cls.get_script_drop_table(field.get_middle_class().get_db_name())
            else:
                return 'ALTER TABLE ' + cls.get_db_name() + ' DROP COLUMN ' + db_field_name
    
    @classmethod
    def get_script_add_field(cls, field_class):
        if field_class in cls.get_fields():
            if field_class.is_many_to_many():
                return field_class.get_script()
            else:
                return 'ALTER TABLE ' + cls.get_db_name() + ' ADD COLUMN ' + field_class.get_script()


    #@classmethod
    #def get_script_drop_table(cls):
    #    return 'DROP TABLE ' + cls.get_db_name()
    

    @classmethod
    def compound_indexes_list(cls):
        ''' must return a list of tuples contaning the indexes : [ ('index_name', False(unique -> True if unique), (field_one, field_two)) ]'''
        return []
    
    @classmethod
    def get_class_name(cls):
        return cls.__name__
    
    @classmethod
    def get_old_class_name(cls):
        return None
    
    @classmethod
    def get_old_db_name(cls):
        return None
    
    @classmethod
    def clear(cls):
        fields = cls.get_fields()
        for field in fields:
            if not field.is_many_to_many():
                field.value = None
            else:
                field.value = []
    
    def __setattr__(self, __name: str, __value) -> None:        
        field = getattr(self, __name, None)
        if field and inspect.isclass(type(field)) and issubclass(type(field), PySqlFieldInterface):
            field.value = __value
        else:
            return super().__setattr__(__name, __value)

class PostgreDbTable(GenericDbTable):
    pass

class MySqlDbTable(GenericDbTable):
    pass

class OracleDbTable(GenericDbTable):
    pass

class SqlServerDbTable(GenericDbTable):
    pass

class SqliteDbTable(GenericDbTable):
    @classmethod
    def get_script_create_table(cls):
        script_create = 'CREATE TABLE ' + cls.get_db_name() + '('
        fields = cls.get_fields()
        
        for field in fields:
            if not field.is_many_to_many():
                script_create += field.get_script() + ', '

        script_create += cls.get_script_create_pk(creating_table=True)[1] + ', '
        
        for fk in cls.get_scripts_fk(creating_table=True):
            script_create += fk[1] + ', '
         
        for ck in cls.get_scripts_check_constraints(creating_table=True):
            script_create += ck[1] + ', '

        script_create = script_create[:-2]
        script_create += ')'
        return script_create

    @classmethod
    def get_script_create_pk(cls, creating_table=False):
        if creating_table:
            fields = cls.get_pk_fields()
            name = ""
            if len(fields) > 0:
                name = 'PK_' + cls.get_db_name()
                script = ' PRIMARY KEY('
                
                for field in fields:
                    script += field.get_db_name() + ', '
            
                script = script[:-2] + ')'
                return (name, script, fields)
            else:
                return ('', '', '')
        else:
            return ('', '', '')

    @classmethod
    def get_scripts_indices(cls, creating_table=False) :
        fields = cls.get_fields()
        name = ''        
        indices = ()
        for field in fields:
            if field.has_unique_index():
                name = 'UX_' + cls.get_db_name() + '_' + field.get_db_name()
                indices += ((name, 'CREATE UNIQUE INDEX ' + name + \
                    ' ON ' + cls.get_db_name() + '('+ field.get_db_name() + ')', (field.get_db_name(), ), True ), )
            elif field.has_normal_index():
                name = 'IX_' + cls.get_db_name() + '_' + field.get_db_name()
                indices += ((name, 'CREATE INDEX ' + name + \
                    ' ON ' + cls.get_db_name() + '('+ field.get_db_name() + ')', (field.get_db_name(), ), False ), )     
        return indices          

    @classmethod
    def get_scripts_fk(cls, creating_table=False):
        if creating_table:
            fields = cls.get_fk_fields()        
            fks = ()
            name = ""
            for field in fields:
                related_pk_fields = field.get_related_to_class().get_pk_fields()
                if len(related_pk_fields) == 0:
                    raise Exception(field.get_related_to_class().get_db_name() + ' has not a primary key.')
                elif len(related_pk_fields) > 1:
                    raise Exception(field.get_related_to_class().get_db_name() + ' has a composite primary key')
                name = 'FK_' + cls.get_db_name()[0:3] + \
                    '_' + field.get_related_to_class().get_db_name()[0:3] + '_' + field.get_db_name() 
                fks += ((name,  ' FOREIGN KEY(' +  field.get_db_name() + ')' + 
                    ' REFERENCES ' + field.get_related_to_class().get_db_name() + 
                    '(' +  related_pk_fields[0].get_db_name() + ')' + field.get_script_cascade(),  field.get_related_to_class().get_db_name()),)
            return fks
        else:
            return ()
    
    @classmethod
    def get_scripts_check_constraints(cls, creating_table = False):
        if creating_table:
            fields = cls.get_fields()
            check = None
            cheks = ()
            name = ''
            for field in fields:
                check = field.get_check_constraint_validation()
                if check:
                    name = 'CK_' + field.get_owner().get_db_name() + '_' + field.get_db_name() 
                    cheks += ((name, ' CHECK (' + check  + ')'), )
            
            return cheks
        else:
            return ()

    @classmethod
    def get_script_remove_field(cls, db_field_name):        
            return 'ALTER TABLE ' + cls.get_db_name() + ' DROP COLUMN ' + db_field_name
    
    @classmethod
    def get_script_add_field(cls, field_class):
        if field_class in cls.get_fields():
            return 'ALTER TABLE ' + cls.get_db_name() + ' ADD COLUMN ' + field_class.get_script()
