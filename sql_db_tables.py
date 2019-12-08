from pysql_class_generator import PySqlClassGenerator


BaseDbTable = type("BaseDbTable", (PySqlClassGenerator.get_db_table_class(), ), {})
