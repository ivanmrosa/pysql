from interface import PySqlOperatorsInterface, PySqlCommandInterface
from field_tools import FieldTools
import inspect


class GenricBaseOperator(object):
    def get_formated_parameter(self, list_of_parameters):
        return '%s'

    def get_value_or_field_name(self, value_or_field, quoted=False, value_as_parameter=False, list_of_parameters=[]):
        if FieldTools.is_db_field(value_or_field):
            return value_or_field.get_owner().get_alias() + '.' + value_or_field.get_db_name()
        else:
            if value_as_parameter:
                list_of_parameters.append(value_or_field)
                return self.get_formated_parameter(list_of_parameters)
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

    def get_sql_text(self, value_as_parameter=False, list_of_parameters=[]):
        return '({field_or_val1} {operator} {field_or_val2})'.format(
            field_or_val1=self.get_value_or_field_name(
                self.value1, True, value_as_parameter, list_of_parameters),
            operator=self.get_operator(),
            field_or_val2=self.get_value_or_field_name(self.value2, True, value_as_parameter, list_of_parameters))


class GenricOdif(GenricOequ):
    def get_operator(self):
        return '<>'


class GenericOnull(PySqlOperatorsInterface, GenricBaseOperator):
    def __init__(self, value):
        self.value = value

    def get_operator(self):
        return 'is null'

    def get_sql_text(self, value_as_parameter=False, list_of_parameters=[]):
        return '({field} {operator})'.format(field=self.get_value_or_field_name(self.value, True, value_as_parameter, list_of_parameters), 
            operator=self.get_operator())

class GenericOnnull(GenericOnull):
    def get_operator(self):
        return 'is not null'


class GenericOor(PySqlOperatorsInterface, GenricBaseOperator):
    def __init__(self, *list_of_comparations):
        self.list_of_comparations = list_of_comparations

    def get_operator(self):
        return 'OR'

    def get_sql_text(self, value_as_parameter=False, list_of_parameters=[]):
        sql = ''
        count = 0
        for condition in self.list_of_comparations:
            sql += condition.get_sql_text(value_as_parameter,
                                          list_of_parameters) + ' AND '
            count += 1

        sql = sql[:-5]

        if count > 1:
            sql = '(' + sql + ')'

        return sql


class GenericOin(PySqlOperatorsInterface, GenricBaseOperator):
    def __init__(self, *list_of_comparations):
        #self.list_of_comparations = list_of_comparations
        if len(list_of_comparations) < 2:
            Exception(
                'oin operator must have unless two parameters. The first one must be the field and the others, values to filter.')

        self.field_to_filter = list_of_comparations[0]
        self.params_to_filter = list_of_comparations[1:]

    def get_operator(self):
        return 'IN'

    def get_sql_text(self, value_as_parameter=False, list_of_parameters=[]):
        is_a_command_class = False

        sql = '{table}.{field} {operator} ('.format(table=self.field_to_filter.get_owner().get_alias(),
                                                    field=self.field_to_filter.get_db_name(), operator=self.get_operator())

        for param in self.params_to_filter:

            if param and inspect.isclass(type(param)):
                is_a_command_class = issubclass(
                    type(param), PySqlCommandInterface)

            if param and not is_a_command_class:
                sql += self.get_value_or_field_name(value_or_field=param, quoted=False,
                                                    value_as_parameter=True, list_of_parameters=list_of_parameters) + ', '
            elif param and is_a_command_class:
                sql += param.get_sql() + '  '
                list_of_parameters += param.list_params

        sql = sql[:-2] + ')'

        return sql


class GenericOnin(GenericOin):

    def get_operator(self):
        return 'NOT IN'


class GenericOlike(GenricOequ):
    def get_operator(self):
        return 'LIKE'


class GenericOnlike(GenricOequ):
    def get_operator(self):
        return 'NOT LIKE'


class GenericObt(GenricOequ):
    def get_operator(self):
        return '>'

class GenericOlt(GenricOequ):
    def get_operator(self):
        return '<'

# postgresql


class GenericOequPostgre(GenricOequ):
    pass


class GenericOdifPostgre(GenricOdif):
    pass


class GenericOnullPostgre(GenericOnull):
    pass

class GenericOnnullPostgre(GenericOnnull):
    pass

class GenericOorPostgre(GenericOor):
    pass


class GenericOinPostgre(GenericOin):
    pass


class GenericOninPostgre(GenericOnin):
    pass


class GenericOlikePostgre(GenericOlike):
    pass


class GenericOnlikePostgre(GenericOnlike):
    pass

class GenericOltPostgre(GenericOlt):
    pass

class GenericObtPostgre(GenericObt):
    pass

# mysql
class GenricOequMySql(GenricOequ):
    pass


class GenricOdifMySql(GenricOdif):
    pass


class GenericOnullMySql(GenericOnull):
    pass

class GenericOnnullMySql(GenericOnnull):
    pass

class GenericOorMySql(GenericOor):
    pass


class GenericOinMysql(GenericOin):
    pass


class GenericOninMysql(GenericOnin):
    pass


class GenericOlikeMysql(GenericOlike):
    pass


class GenericOnlikeMysql(GenericOnlike):
    pass

class GenericOltMysql(GenericOlt):
    pass

class GenericObtMysql(GenericObt):
    pass



# oracle
class GenricOequOracle(GenricOequ):
    pass


class GenricOdifOracle(GenricOdif):
    pass


class GenericOnullOracle(GenericOnull):
    pass

class GenericOnnullOracle(GenericOnnull):
    pass

class GenericOorOracle(GenericOor):
    pass


class GenericOinOracle(GenericOin):
    pass


class GenericOninOracle(GenericOnin):
    pass


class GenericOlikeOracle(GenericOlike):
    pass


class GenericOnlikeOracle(GenericOnlike):
    pass

class GenericOltOracle(GenericOlt):
    pass

class GenericObtOracle(GenericObt):
    pass

# sqlserver

class GenricBaseOperatorSqlServer(GenricBaseOperator):
    pass


class GenricOequSqlServer(GenricOequ):
    pass


class GenricOdifSqlServer(GenricOdif):
    pass


class GenericOnullSqlServer(GenericOnull):
    pass

class GenericOnnullSqlServer(GenericOnnull):
    pass

class GenericOorSqlServer(GenericOor):
    pass


class GenericOinSqlServer(GenericOin):
    pass


class GenericOninSqlServer(GenericOnin):
    pass


class GenericOlikeSqlServer(GenericOlike):
    pass


class GenericOnlikeSqlServer(GenericOnlike):
    pass

class GenericOltSqlServer(GenericOlt):
    pass

class GenericObtSqlServer(GenericObt):
    pass
