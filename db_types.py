from pysql_config import DB_DRIVER, DRIVER_CLASSES_CONFIG
from interface import PySqlFieldInterface
import inspect 


class NullValue(object):
    pass

class CurrentDate(object):
    pass


class Field(PySqlFieldInterface):
    __is_db_field = True
    __owner = None
    def __init__(self, db_name, nullable = True, index = False, primary_key = False, \
        unique = False, size=0, scale = 0, precision=0, default=None, permitted_values=()):
        
        self._index = index
        self._db_name = db_name
        self._primary_key = primary_key
        self._unique = unique
        self._value = None
        self._scale = scale
        self._precision = precision
        self._size = size
        self._nullable = nullable
        self._default = default
        self._permitted_values = permitted_values
        self._value_is_string = False
    
    def get_db_name(self):
        return self._db_name
    
    def get_generic_type_name(self):
        return type(self).__name__
    
    def is_primary_key(self):
        return self._primary_key

    def has_unique_index(self):
        return self._unique

    def has_normal_index(self):
        return self._index

    def is_foreign_key(self):
        return False
    
    def get_owner(self):
        return self.__owner
        
    def __set_name__(self, owner, name):
        self.__owner = owner
    
    def get_prepared_default_value(self):
        return self.get_prepared_value_to_script(self._default)
    
    def get_prepared_value_to_script(self, value):
        if inspect.isclass(value):
            if issubclass(value, NullValue):
                return 'Null'
            elif issubclass(value, CurrentDate):
                return DRIVER_CLASSES_CONFIG[DB_DRIVER]['CURRENT_DATE_TYPE']
        
        if self._value_is_string:
            return '\'' + value + '\''
        else:
            return value

    def get_check_constraint_validation(self):
        script = ''
        if len(self._permitted_values) > 0:
            for value in self._permitted_values:
                script += self.get_db_name() + ' = ' +  self.get_prepared_value_to_script(value) + ' OR '
                        
            return script[:-4]
        else:
            return None 

    def get_field_type_and_configureted(self):
        config = DRIVER_CLASSES_CONFIG[DB_DRIVER]
        field_config = config['FIELDS_CONFIG'][self.get_generic_type_name()]
        script = field_config['NAME']
        if field_config['HAS_SIZE']:
            script += '(' + str(self._size) + ')'
        elif field_config['HAS_PRECISION'] or field_config['HAS_SCALE']:
            script += '(' + str(self._precision) + ', ' + str(self._scale) + ')'
        
        if not self._nullable or self._primary_key:
            script += ' NOT NULL'
        
        if self._default != None:
            script += ' DEFAULT ' + self.get_prepared_default_value()
        
        return script


    def get_script(self):                        
        return self.get_db_name() + ' ' + self.get_field_type_and_configureted()
    
    @property
    def value(self):
        return self._value
    
    @value.setter
    def value(self, val):
        self._value = val



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
                        
        
        self._scale = pk_fields[0]._scale
        self._precision = pk_fields[0]._precision
        self._size = pk_fields[0]._size
        self._permitted_values = pk_fields[0]._permitted_values                    
        script = self.get_db_name() + ' ' + self.get_field_type_and_configureted()
        
        return script

    def get_generic_type_name(self):
        field_type = type(self.__related_to_class.get_pk_fields()[0])

        if issubclass(field_type, IntegerPrimaryKey):
            return IntegerField.__name__
        else:
            return field_type.__name__
            

    def is_foreign_key(self):
        return True
        
        
class IntegerField(Field):
    def __init__(self, db_name, nullable = True, index = False, primary_key = False, \
        unique = False, size=0, scale = 0, precision=0, default=None, permitted_values=()):
        super(IntegerField, self).__init__(index=index, db_name=db_name, primary_key=primary_key, unique=unique, 
        nullable=nullable, default=default, permitted_values=permitted_values)   


class SmallIntField(IntegerField):
    pass

class BigIntField(IntegerField):
    pass

class NumericField(Field):
    pass

class MoneyField(Field):
    pass

class CharacterField(Field):
    def __init__(self, db_name, size, index = False, primary_key = False, unique = False, nullable = True, default=None, permitted_values=()):
        super(CharacterField, self).__init__(index=index, db_name=db_name, primary_key=primary_key, unique=unique, 
            size = size, nullable=nullable, default=default, permitted_values=permitted_values, precision=0, scale=0)   
        self._value_is_string = True     
    
class VarcharField(CharacterField):
    pass

class TextField(CharacterField):
    pass

class IntegerPrimaryKey(IntegerField):
    def __init__(self, db_name='id'):
        super().__init__(db_name=db_name, nullable=False, index=False, primary_key=True)


class DateField(Field):
    def __init__(self, db_name, index = False, primary_key = False, unique = False, nullable = True, default=None, permitted_values=()):
        super(DateField, self).__init__(index=index, db_name=db_name, primary_key=primary_key, unique=unique, 
            size=0 , nullable=nullable, default=default, permitted_values=permitted_values, precision=0, scale=0)   
    

class DateTimeField(DateField):
    pass