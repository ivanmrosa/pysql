from sql_db_tables import BaseDbTable
from db_types import ForeignKey, IntegerField, VarcharField, MoneyField, CharacterField
from models.estado import Estado

class Cidade(BaseDbTable):
    id = IntegerField(primary_key=True, db_name='id')
    estado = ForeignKey(related_to_class=Estado) 
    nome = VarcharField(size=50, db_name='nome', nullable=False)   