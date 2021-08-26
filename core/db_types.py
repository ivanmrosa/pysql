from . pysql_config import DB_DRIVER, DRIVER_CLASSES_CONFIG
from . interface import PySqlFieldInterface
import inspect 
from copy import deepcopy
import datetime


class NullValue(object):
    pass

class CurrentDate(object):
    pass


class Field(PySqlFieldInterface):
    __is_db_field = True
    __owner = None
    def __init__(self, db_name = '', nullable = True, index = False, primary_key = False, \
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

        self.__temporary_alias = ''
        self.__temporary_text_function = ''
        self.__used_in_aggregated_function = False
        self.__is_deep_copy = False

        self._property_name = ''

    @classmethod
    def get_class_name(cls):
        return cls.__name__
    
    def get_db_name(self):
        return self._db_name
    
    def set_db_name(self, db_name):
        self._db_name = db_name
    
    def get_alias(self):
        if self.__temporary_alias:
            return self.__temporary_alias
        else:     
            if self._property_name and self._property_name != self._db_name:
                return self._property_name
            else:
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
    
    def is_many_to_many(self):
        return False
    
    def get_owner(self):
        return self.__owner
        
    def set_owner(self, owner):
        self.__owner = owner

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
    
    def is_used_in_aggregated_function(self):
        return self.__used_in_aggregated_function
        
    def get_field_configureted_for_functions(self, text_function, alias, is_aggregated):
        ''' pass function using the pattern {field} where the field should be used
          Ex.: fsubstr({field}, 1 ,6) -> copy the field value from first position to 6 next sixth positions 
        '''
        self.__temporary_alias = alias
        if self.__is_deep_copy:
            self.__temporary_text_function = text_function.format(field=self.__temporary_text_function)
        else:
            self.__temporary_text_function = text_function
        
        self.__is_deep_copy = True
        
        if not self.__used_in_aggregated_function:
            self.__used_in_aggregated_function = is_aggregated
        
        new_field = deepcopy(self)
        self.__temporary_alias = ''
        self.__temporary_text_function = ''
        self.__used_in_aggregated_function = False
        self.__is_deep_copy = False
        return new_field
    
    def get_field_alias_key(self):
        return ' AS '
    
    def get_sql_for_field(self, use_alias = True):
        standard_text = '{table_name}.{field_name}'.format(table_name=self.get_owner().get_alias(),  field_name=self.get_db_name())
        if self.__temporary_text_function:        
            
            if '{field}' in self.__temporary_text_function:    
                text = self.__temporary_text_function.format(field=standard_text) + ' '
            else:
                text = self.__temporary_text_function
            
            if use_alias:
                text +=  self.get_field_alias_key() + self.get_alias()
        else:        
            text = standard_text       
            if use_alias and self.get_alias() != self.get_db_name():
                text +=  self.get_field_alias_key() + self.get_alias()
        
        self.__temporary_alias = ''
        self.__temporary_text_function = ''
        return text 
        
    @property
    def value(self):
        return self._value
    
    @value.setter
    def value(self, val):
        self._value = val

    #def __get__(self, instance, owner):
    #    return self._value
    
    #def __set__(self, instance, value):
    #    self._value = value



class ForeignKey(Field):
    def __init__(self, related_to_class, nullable=True, db_name='', index=False, unique=False, delete_cascade=False):        
        super(ForeignKey, self).__init__(db_name= db_name, nullable=nullable, index=index, primary_key=False, unique=unique)        
        self.__related_to_class = related_to_class
        self.delete_cascade = delete_cascade
            
    #def get_related_class(self):
    #    return self.__related_to_class

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
    
    def get_script_cascade(self):
        if self.delete_cascade:
            return ' ON DELETE CASCADE '
        else:
            return ''

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


class ManyToManyField(Field):
    
    def __init__(self, related_to_class, unique=False, db_name='', nullable=True):
        super(ManyToManyField, self).__init__(db_name=db_name, nullable=nullable, index=False, primary_key=False, unique=unique)
        self.__related_to_class = related_to_class        
        self.__many_to_many_table = None
        
        
    def add(self, pk_related_class):
        self.value.append(pk_related_class)
    
    def get_related_class(self):
        return self.__related_to_class

    def get_related_to_class(self):
        return self.__related_to_class

    def get_db_name(self):               
        return 'mtm_' + self.get_owner().get_db_name() + '_' + self.__related_to_class.get_db_name()
    
    def get_middle_class(self):
        if self.__many_to_many_table:
            return self.__many_to_many_table

        pk_fields = self.__related_to_class.get_pk_fields()
        pk_fields_this_class = self.get_owner().get_pk_fields()
        related_table_name = self.__related_to_class.get_db_name()        

        if len(pk_fields) != 1 or len(pk_fields_this_class) != 1:
            raise Exception('Impossible to create a many to many field for ' + self.get_alias() + 
            ' to relate with ' + related_table_name + '. Too many primary key fields or no primary key found')

        fields = {"id": IntegerPrimaryKey()}        
        fields.update({pk_fields_this_class[0].get_alias() + '_' + self.get_owner().get_alias(): ForeignKey(related_to_class=self.get_owner(), index=True, delete_cascade=True, nullable=False) })        
        fields.update({pk_fields[0].get_alias() + '_' + pk_fields[0].get_owner().get_alias(): ForeignKey(related_to_class=pk_fields[0].get_owner(), index=True, delete_cascade=True, nullable=False) })
        
        
        self.__many_to_many_table = type(self.get_db_name(), (DRIVER_CLASSES_CONFIG[DB_DRIVER]['DB_TABLE_CLASS'], ), fields)
        return self.__many_to_many_table
                

    def get_script(self):
        return self.get_middle_class().get_script_create_table()      
        
    
    def get_generic_type_name(self):
        field_type = type(self.__related_to_class.get_pk_fields()[0])

        if issubclass(field_type, IntegerPrimaryKey):
            return IntegerField.__name__
        else:
            return field_type.__name__
    
    def set_alias(self, alias):
        self.get_middle_class().set_alias(alias)

    def is_many_to_many(self):
        return True
        
class IntegerField(Field):
    def __init__(self, db_name = '', nullable = True, index = False, primary_key = False, \
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

class FloatField(Field):
    pass

class CharacterField(Field):
    def __init__(self, size, db_name = '', index = False, primary_key = False, unique = False, nullable = True, default=None, permitted_values=()):
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
    def __init__(self, db_name = '', index = False, primary_key = False, unique = False, nullable = True, default=None, permitted_values=()):
        super(DateField, self).__init__(index=index, db_name=db_name, primary_key=primary_key, unique=unique, 
            size=0 , nullable=nullable, default=default, permitted_values=permitted_values, precision=0, scale=0)
    
    @property
    def value(self):
        if self._value:
            return datetime.datetime.fromisoformat(self._value)    
        else:
            return self._value    
    
    @value.setter
    def value(self, val):
        
        if type(val) in (datetime.datetime, datetime.date):
            self._value = datetime.date(val.year, val.month, val.day).isoformat()
        else:
            if not val:
                self._value = val
            else:
                raise Exception('Date must be a valid python date object')
    

class DateTimeField(DateField):
    @property
    def value(self):
        if self._value:
            return datetime.datetime.fromisoformat(self._value)    
        else:
            return self._value 
       
    @value.setter
    def value(self, val):
        
        if type(val) in (datetime.datetime, datetime.date) :
            self._value = val.isoformat()
        else:
            if not val:
                self._value = val
            else:
                raise Exception('Date must be a valid python date object')


