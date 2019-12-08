from sql_db_tables import BaseDbTable
from db_types import ForeignKey, IntegerField, VarcharField, MoneyField, CharacterField

class Pais(BaseDbTable):
    id = IntegerField(primary_key=True, db_name='id')
    nome = VarcharField(size=50, db_name='nome', unique=True, nullable = False)
    codigo = IntegerField(db_name='codigo', index=True)
