from sql_db_tables import BaseDbTable
from test.models.marca_fipe import MarcaFipe
from db_types import VarcharField, IntegerPrimaryKey, ForeignKey, IntegerField
class ModeloFipe(BaseDbTable):
    id = IntegerPrimaryKey()
    marca_fipe = ForeignKey(related_to_class=MarcaFipe, nullable=False)
    nome = VarcharField(db_name='nome', size=250, index=True, nullable=False)
    ano = IntegerField(db_name='ano')
    
    @classmethod
    def get_db_name(cls):
        return 'modelo_fipe'
    
    @classmethod
    def compound_indexes_list(cls):
        return [('ix_modelo_fipe_marca_nome', False, (cls.marca_fipe, cls.nome))]
    
