from pysql_class_generator import PySqlClassGenerator

operatorConfig = PySqlClassGenerator.get_db_operators_classes()
class_equal = operatorConfig['equal']
class_different = operatorConfig['different']
class_null = operatorConfig['is_null']
class_or = operatorConfig['or']

class oequ(class_equal):
    pass

class odif(class_different):
    pass

class onull(class_null):
    pass

class oor(class_or):
    pass


