from sql_db_tables import BaseDbTable
from db_types import ForeignKey, IntegerField, VarcharField, MoneyField, CharacterField, IntegerPrimaryKey
from test.models.pais import Pais

class Estado(BaseDbTable):
    id = IntegerPrimaryKey()
    pais = ForeignKey(related_to_class=Pais, nullable=False)
    nome = VarcharField(size=50, db_name='nome', nullable=False)
    sigla = VarcharField(size=2, db_name='sigla')