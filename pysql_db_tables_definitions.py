from interface import PySqlDatabaseTableInterface
#from db_types import  
#from db_types import ForeignKey
import inspect

class GenericDbTable(PySqlDatabaseTableInterface):
    __alias = None
    __db_name = None
    __pk_fields = ()
    __compound_indexes = []

    @classmethod
    def get_alias(cls):
        if not cls.__alias:
            cls.set_alias(cls.__name__)
        
        return cls.__alias

    @classmethod
    def set_alias(cls, alias):
        cls.__alias = alias

    @classmethod
    def get_db_name(cls):
        if not cls.__db_name:
            cls.__db_name = cls.__name__
        return cls.__db_name
    
    @classmethod
    def get_copy(cls, alias):
        #copy = type(alias, cls.__bases__, dict(cls.__dict__))
        copy = type(alias, (cls, ), {})
        copy.set_alias(alias)
        copy.__db_name = cls.get_db_name()        
        return copy
        
    @classmethod
    def get_fields(cls):
        attributes = cls.__dict__
        fields = []
        for attr in attributes.keys():
            item = attributes.__getitem__(attr)
            #if not inspect.isroutine(item) and inspect.isclass(type(item)):
            if not(attr.startswith('__') and attr.endswith('__')) and getattr(item, '_Field__is_db_field', False):
                fields.append(item)
        return fields

    @classmethod
    def get_script_create_table(cls):
        script_create = 'CREATE TABLE ' + cls.get_db_name() + '('
        fields = cls.get_fields()
        
        for field in fields:
            script_create += field.get_script() + ', '
        
        script_create = script_create[:-2]
        script_create += ')'
        return script_create
    
    @classmethod
    def get_script_create_pk(cls):
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
    def get_scripts_indices(cls):
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
    def get_scripts_fk(cls):
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
                '(' +  related_pk_fields[0].get_db_name() + ')',  field.get_related_to_class().get_db_name()),)
        return fks

    @classmethod
    def get_script_remove_field(cls, db_field_name):        
            return 'ALTER TABLE ' + cls.get_db_name() + ' DROP COLUMN ' + db_field_name
    
    @classmethod
    def get_script_add_field(cls, field_class):
        if field_class in cls.get_fields():
            return 'ALTER TABLE ' + cls.get_db_name() + ' ADD COLUMN ' + field_class.get_script()


    @classmethod
    def get_script_drop_table(cls):
        return 'DROP TABLE ' + cls.get_db_name()
    

    @classmethod
    def compound_indexes_list(cls):
        ''' must return a list of tuples contaning the indexes : [ ('index_name', False(unique -> True if unique), (field_one, field_two)) ]'''
        return []
    
    @classmethod
    def get_old_class_name(cls):
        return None
    
    @classmethod
    def get_old_db_name(cls):
        return None

       
class PostgreDbTable(GenericDbTable):
    pass

class MySqlDbTable(GenericDbTable):
    pass

class OracleDbTable(GenericDbTable):
    pass

class SqlServerDbTable(GenericDbTable):
    pass