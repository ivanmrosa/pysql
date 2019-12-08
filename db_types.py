from pysql_config import DB_DRIVER, DRIVER_CLASSES_CONFIG
from interface import PySqlFieldInterface
import inspect 


class NullValue(object):
    pass

class Field(PySqlFieldInterface):
    __is_db_field = True
    __owner = None
    def __init__(self, db_name, nullable = True, index = False, primary_key = False, \
        unique = False, size=0, scale = 0, precision=0, default=None):
        
        self.__index = index
        self.__db_name = db_name
        self.__primary_key = primary_key
        self.__unique = unique
        self.__value = None
        self.__scale = scale
        self.__precision = precision
        self.__size = size
        self.__nullable = nullable
        self.__default = default
    
    def get_db_name(self):
        return self.__db_name
    
    def get_generic_type_name(self):
        return type(self).__name__
    
    def is_primary_key(self):
        return self.__primary_key

    def has_unique_index(self):
        return self.__unique

    def has_normal_index(self):
        return self.__index

    def is_foreign_key(self):
        return False
    
    def get_owner(self):
        return self.__owner
        
    def __set_name__(self, owner, name):
        self.__owner = owner

    def get_field_type_and_configureted(self):
        config = DRIVER_CLASSES_CONFIG[DB_DRIVER]
        field_config = config['FIELDS_CONFIG'][self.get_generic_type_name()]
        script = field_config['NAME']
        if field_config['HAS_SIZE']:
            script += '(' + str(self.__size) + ')'
        elif field_config['HAS_PRECISION'] or field_config['HAS_SCALE']:
            script += '(' + self.__precision + ', ' + self.__scale + ')'
        
        if not self.__nullable or self.__primary_key:
            script += ' NOT NULL'
        
        return script


    def get_script(self):                        
        return self.get_db_name() + ' ' + self.get_field_type_and_configureted()
    
    @property
    def value(self):
        return self.__value
    
    @value.setter
    def value(self, val):
        self.__value = val


class ForeignKey(Field):
    def __init__(self, related_to_class, nullable=True, db_name='', index=False, unique=False):        
        super(ForeignKey, self).__init__(db_name= db_name, nullable=nullable, index=index, primary_key=False, unique=unique)        
        self.__related_to_class = related_to_class
            
    def get_related_class(self):
        return self.__related_to_class

    def get_related_to_class(self):
        return self.__related_to_class

    def get_db_name(self):
        pk_fields = self.__related_to_class.get_pk_fields()        
        related_table_name = self.__related_to_class.get_db_name()        
        if super().get_db_name():
            field_name = related_table_name + '_' + super().get_db_name()
        else:
            field_name = related_table_name + '_' + pk_fields[0].get_db_name()
        
        return field_name

    def get_script(self):
        pk_fields = self.__related_to_class.get_pk_fields()
        related_table_name = self.__related_to_class.get_db_name()        

        if len(pk_fields) > 1 or len(pk_fields) == 0:
            raise Exception('Impossible to create a foreign key for ' + self.get_db_name() + 
            ' to relate with ' + related_table_name)
        
            
        script = self.get_db_name() + ' ' + pk_fields[0].get_field_type_and_configureted()
        
        return script

    def is_foreign_key(self):
        return True
        
        
class IntegerField(Field):
    pass

class SmallIntField(Field):
    pass

class BigIntField(Field):
    pass

class NumericField(Field):
    pass

class MoneyField(Field):
    pass

class CharacterField(Field):
    def __init__(self, db_name, size, index = False, primary_key = False, unique = False, nullable = True):
        super(CharacterField, self).__init__(index=index, db_name=db_name, primary_key=primary_key, unique=unique, 
            size = size, nullable=nullable)        

class VarcharField(CharacterField):
    pass

class TextField(Field):
    pass

class IntegerPrimaryKey(IntegerField):
    def __init__(self, db_name='id'):
        super().__init__(db_name=db_name, nullable=False, index=False, primary_key=True)


