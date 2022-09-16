from . pysql_class_generator import PySqlClassGenerator
from . pysql_db_tables_definitions import GenericDbTable

BaseDbTable : GenericDbTable = type("BaseDbTable", (PySqlClassGenerator.get_db_table_class(), ), {})
