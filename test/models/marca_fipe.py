from sql_db_tables import BaseDbTable
from db_types import VarcharField, IntegerPrimaryKey

class MarcaFipe(BaseDbTable):
    id = IntegerPrimaryKey()
    nome = VarcharField(db_name='nome', size=50, index=True, nullable=False)
    
    @classmethod
    def get_db_name(cls):
        return 'marca_fipe'
    
