from sql_db_tables import BaseDbTable
from models.fonte_de_anuncio import FonteDeAnuncio
from models.modelo_veiculo import ModeloVeiculo
from models.marca_veiculo import MarcaVeiculo
from db_types import IntegerPrimaryKey, VarcharField, TextField, ForeignKey, IntegerField, NumericField

class Anuncio(BaseDbTable):
    id = IntegerPrimaryKey()
    titulo = VarcharField(db_name='titulo', size=255, nullable=False)
    descricao = VarcharField(db_name='descricao', size=1000)
    modelo = ForeignKey(ModeloVeiculo, index=True)
    ano_fabricacao = IntegerField(db_name='ano_fabricacao')
    ano_modelo = IntegerField(db_name='ano_modelo')
    quilometragem = IntegerField(db_name='quilometragem')
    