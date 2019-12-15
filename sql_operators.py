from pysql_class_generator import PySqlClassGenerator

operatorConfig = PySqlClassGenerator.get_db_operators_classes()
class_equal = type('class_equal', (operatorConfig['equal'], ), {})
class_different = type('class_different', (operatorConfig['different'], ), {}) 
class_null = type('class_null', (operatorConfig['is_null'], ), {})  
class_or = type('class_or', (operatorConfig['or'], ), {})   
class_in = type('class_in', (operatorConfig['in'], ), {})    
class_not_in = type('class_not_in', (operatorConfig['not_in'], ), {})    

class oequ(class_equal):
    pass

class odif(class_different):
    pass

class onull(class_null):
    pass

class oor(class_or):
    pass

class oin(class_in):
    pass

class onin(class_not_in):
    pass