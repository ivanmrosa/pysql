from pysql_class_generator import PySqlClassGenerator

operatorConfig = PySqlClassGenerator.get_db_operators_classes()
oequ = type('oequ', (operatorConfig['equal'], ), {})
odif = type('odif', (operatorConfig['different'], ), {})
onull = type('onull', (operatorConfig['is_null'], ), {})
onnull = type('onnull', (operatorConfig['is_not_null'], ), {})
oor = type('oor', (operatorConfig['or'], ), {})
oin = type('oin', (operatorConfig['in'], ), {})
onin = type('onin', (operatorConfig['not_in'], ), {})
olike = type('olike', (operatorConfig['like'], ), {})
onlike = type('onlike', (operatorConfig['not_like'], ), {})
