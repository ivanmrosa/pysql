from db_types import Field
from field_tools import FieldTools
from pysql_class_generator import PySqlClassGenerator
from interface import PySqlDistinctClause
import inspect

class DistinctType(PySqlDistinctClause):
    def __init__(self, fields):
        self.fields = fields
    def get_fields(self):
        return self.fields

def fdistinct(*fields):
    return DistinctType(fields)

def fsum(field, alias = ''):
    text_function = PySqlClassGenerator.get_sql_functions_config_class().fsum()
    return field.get_field_configureted_for_functions(text_function, alias, True)
    
def favg(field, alias = ''): 
    text_function = PySqlClassGenerator.get_sql_functions_config_class().favg()
    return field.get_field_configureted_for_functions(text_function, alias, True)

def fcount(field = None, alias = ''): 
    text_function = PySqlClassGenerator.get_sql_functions_config_class().fcount()    
    if field:
        if FieldTools.is_db_field(field):
            return field.get_field_configureted_for_functions(text_function, alias, True)
        elif issubclass(type(field), PySqlDistinctClause):
            text_function = PySqlClassGenerator.get_sql_functions_config_class().fcount(True)
            subfield = field.get_fields()[0]
            return subfield.get_field_configureted_for_functions(text_function, alias, True)
    else:
        f = Field(db_name=alias)        
        f.get_sql_for_field = lambda : text_function.format(field='*')
        return f

def fmax(field, alias = ''): 
    text_function = PySqlClassGenerator.get_sql_functions_config_class().fmax()
    return field.get_field_configureted_for_functions(text_function, alias, True)

def fmin(field, alias = ''): 
    text_function = PySqlClassGenerator.get_sql_functions_config_class().fmin()
    return field.get_field_configureted_for_functions(text_function, alias, True)

def fupper(field, alias = ''): 
    text_function = PySqlClassGenerator.get_sql_functions_config_class().fupper()
    return field.get_field_configureted_for_functions(text_function, alias, False)

def flower(field, alias = ''): 
    text_function = PySqlClassGenerator.get_sql_functions_config_class().flower()
    return field.get_field_configureted_for_functions(text_function, alias, False)

def fsubstr(field, pos_initial, length, alias = ''): 
    text_function = PySqlClassGenerator.get_sql_functions_config_class().fsubstr(pos_initial, length)
    return field.get_field_configureted_for_functions(text_function, alias, False)

def ftrim(field, character = ' ', alias = ''): 
    
    if FieldTools.is_db_field(character):
        temp_character = character.get_sql_for_field()
    else:
        temp_character = "'" + character + "'"

    text_function = PySqlClassGenerator.get_sql_functions_config_class().ftrim(temp_character)
    return field.get_field_configureted_for_functions(text_function, alias, False)

def fltrim(field, character = ' ', alias = ''): 
    if FieldTools.is_db_field(character):
        temp_character = character.get_sql_for_field()
    else:
        temp_character = "'" + character + "'"

    text_function = PySqlClassGenerator.get_sql_functions_config_class().fltrim(temp_character)
    return field.get_field_configureted_for_functions(text_function, alias, False)

def frtrim(field, character = ' ', alias = ''): 
    if FieldTools.is_db_field(character):
        temp_character = character.get_sql_for_field()
    else:
        temp_character = "'" + character + "'"

    text_function = PySqlClassGenerator.get_sql_functions_config_class().frtrim(temp_character)
    return field.get_field_configureted_for_functions(text_function, alias, False)

def flength(field, alias = ''): 
    text_function = PySqlClassGenerator.get_sql_functions_config_class().flength()
    return field.get_field_configureted_for_functions(text_function, alias, False)

def freplace(field, replace_this, replace_to, alias = ''):     
    if FieldTools.is_db_field(replace_this):
        temp_replace_this = replace_this.get_sql_for_field()
    else:
        temp_replace_this = "'{character}'".format(character=replace_this)
    
    if FieldTools.is_db_field(replace_to):
        temp_replace_to = replace_to.get_sql_for_field()
    else:
        temp_replace_to = "'{character}'".format(character=replace_to)

    text_function = PySqlClassGenerator.get_sql_functions_config_class().freplace(temp_replace_this, temp_replace_to)
    return field.get_field_configureted_for_functions(text_function, alias, False)

def finstr(field, substring, alias = ''):     
    if FieldTools.is_db_field(substring):
        temp_substring = temp_substring.get_sql_for_field()
    else:
        temp_substring = "'{character}'".format(character=substring)

    text_function = PySqlClassGenerator.get_sql_functions_config_class().finstr(temp_substring)
    return field.get_field_configureted_for_functions(text_function, alias, False)

def fconcat(*fields, alias = ''):
    raise Exception('fconcat is not implemented.')

def frpad(field, complete_with, size, alias = ''):
    text_function = PySqlClassGenerator.get_sql_functions_config_class().frpad(complete_with, size)
    return field.get_field_configureted_for_functions(text_function, alias, False)


def flpad(field, complete_with, size, alias = ''):
    text_function = PySqlClassGenerator.get_sql_functions_config_class().flpad(complete_with, size)
    return field.get_field_configureted_for_functions(text_function, alias, False)
