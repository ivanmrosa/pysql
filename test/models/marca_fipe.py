from core.sql_db_tables import BaseDbTable
from core.db_types import VarcharField, IntegerPrimaryKey

class MarcaFipe(BaseDbTable):
    id = IntegerPrimaryKey()
    nome = VarcharField(db_name='nome_fipe', size=50, index=True, nullable=False)
    
    @classmethod
    def get_db_name(cls):
        return 'marca_fipe'
    
