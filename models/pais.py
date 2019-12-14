from sql_db_tables import BaseDbTable
from db_types import ForeignKey, IntegerField, VarcharField, MoneyField, CharacterField, IntegerPrimaryKey

class Pais(BaseDbTable):
    id = IntegerPrimaryKey()
    nome = VarcharField(size=50, db_name='nome', unique=True, nullable = False)
    codigo = IntegerField(db_name='codigo', index=True)
