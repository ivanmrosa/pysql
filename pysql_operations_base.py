from interface import PySqlOperatorsInterface
from field_tools import FieldTools
import inspect

class GenricBaseOperator(object):
    def get_formated_parameter(self, list_of_parameters):
        return '%s' 

    def get_value_or_field_name(self, value_or_field, quoted = False, value_as_parameter = False, list_of_parameters = []):
        if FieldTools.is_db_field(value_or_field):
            return value_or_field.get_owner().get_alias() + '.' + value_or_field.get_db_name()
        else:
            if value_as_parameter:
                list_of_parameters.append(value_or_field)
                return  self.get_formated_parameter(list_of_parameters)
            else:    
                if quoted and type(value_or_field) is str:
                    return "'" + value_or_field + "'"
                else:
                    return value_or_field

class GenricOequ(PySqlOperatorsInterface, GenricBaseOperator):
    def __init__(self, value1, value2):
        self.value1 = value1
        self.value2 = value2

    def get_operator(self):
        return '='

    def get_sql_text(self, value_as_parameter = False, list_of_parameters = []):
        return '({field_or_val1} {operator} {field_or_val2})'.format( \
            field_or_val1=self.get_value_or_field_name(self.value1, True, value_as_parameter, list_of_parameters),
            operator=self.get_operator(), 
            field_or_val2=self.get_value_or_field_name(self.value2, True, value_as_parameter, list_of_parameters) )
    
class GenricOdif(GenricOequ):
    def get_operator(self):
        return '<>'

class GenericOnull(PySqlOperatorsInterface, GenricBaseOperator):
    def __init__(self, value):
        self.value = value

    def get_operator(self):
        return 'is'
    
    def get_sql_text(self, value_as_parameter = False, list_of_parameters = []):
        return '({field} is null)'.format(self.get_value_or_field_name(self.value, True, value_as_parameter, list_of_parameters))


class GenericOor(PySqlOperatorsInterface, GenricBaseOperator):
    def __init__(self, *list_of_comparations):
        self.list_of_comparations = list_of_comparations
    
    def get_operator(self):
        return 'OR'

    def get_sql_text(self, value_as_parameter = False, list_of_parameters = []):
        sql = ''
        count = 0
        for condition in self.list_of_comparations:
            sql += condition.get_sql_text(value_as_parameter, list_of_parameters) + ' AND '
            count += 1
        

        sql = sql[:-5]

        if count > 1:
            sql = '(' + sql + ')'

        return sql



#postgresql
class GenericOequPostgre(GenricOequ):
  pass

class GenericOdifPostgre(GenricOdif):
  pass

class GenericOnullPostgre(GenericOnull):
   pass

class GenericOorPostgre(GenericOor):
   pass


#mysql
class GenricOequMySql(GenricOequ):
  pass

class GenricOdifMySql(GenricOdif):
  pass

class GenericOnullMySql(GenericOnull):
   pass

class GenericOorMySql(GenericOor):
   pass



#oracle
class GenricOequOracle(GenricOequ):
  pass

class GenricOdifOracle(GenricOdif):
  pass

class GenericOnullOracle(GenericOnull):
   pass

class GenericOorOracle(GenericOor):
   pass


#sqlserver

class GenricBaseOperatorSqlServer(GenricBaseOperator):
    pass

class GenricOequSqlServer(GenricOequ):
  pass

class GenricOdifSqlServer(GenricOdif):
  pass

class GenericOnullSqlServer(GenericOnull):
   pass

class GenericOorSqlServer(GenericOor):
   pass
