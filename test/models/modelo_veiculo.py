from sql_db_tables import BaseDbTable
from db_types import VarcharField, IntegerPrimaryKey, ForeignKey
from test.models.marca_veiculo import MarcaVeiculo

class ModeloVeiculo(BaseDbTable):
    id = IntegerPrimaryKey()
    marca = ForeignKey(related_to_class=MarcaVeiculo, index=True)
    modelo = VarcharField(db_name="modelo", size= 250, index=True)
    codigo_integracao = VarcharField(db_name='codigo_integracao', size=10)
    
    @classmethod
    def get_db_name(cls):
        return 'modelo_veiculo'
