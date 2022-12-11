from core.sql_db_tables import BaseDbTable
from core.db_types import IntegerPrimaryKey, DateTimeField, VarcharField, TextField, ForeignKey, CurrentDate

class PySQLMigration(BaseDbTable):
    Id = IntegerPrimaryKey()
    Date = DateTimeField(default=CurrentDate)
    Description = VarcharField(size=300)    


class PySQLStructure(BaseDbTable):
    Id = IntegerPrimaryKey()
    ObjectName = VarcharField(size=255)
    Structure = TextField(size=4000)
    Migration = ForeignKey(PySQLMigration)
