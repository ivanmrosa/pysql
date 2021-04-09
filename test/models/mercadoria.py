from core.sql_db_tables import BaseDbTable
from core.db_types import IntegerPrimaryKey, VarcharField, TextField, ForeignKey, IntegerField, NumericField, CharacterField, NullValue, DateField, CurrentDate, \
    FloatField, ManyToManyField

class Produto(BaseDbTable):
    id = IntegerPrimaryKey()
    nome = VarcharField(size=100, db_name='nome')
    categoria = VarcharField(size=100, db_name='categoria')
    valor_unitario = NumericField(db_name='valor_unitario', scale=2, precision=20)

class Venda(BaseDbTable):
    id = IntegerPrimaryKey()
    produto = ForeignKey(Produto, nullable=True)
    quantidade = IntegerField(db_name='quantidade')

class VendaMultipla(BaseDbTable):
    id = IntegerPrimaryKey()
    data_venda = DateField()
    cancelada = CharacterField(size=1, permitted_values=('T', 'F'), default='F')
    produtos = ManyToManyField(related_to_class=Produto)
