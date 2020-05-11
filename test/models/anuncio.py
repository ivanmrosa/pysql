from core.sql_db_tables import BaseDbTable
from test.models.fonte_de_anuncio import FonteDeAnuncio
from test.models.modelo_veiculo import ModeloVeiculo
from test.models.marca_veiculo import MarcaVeiculo
from test.models.cidade import Cidade
from core.db_types import IntegerPrimaryKey, VarcharField, TextField, ForeignKey, IntegerField, NumericField, CharacterField, NullValue, DateField, CurrentDate

CAMBIO_AUTOMATICO = 'A'
CAMBIO_MANUAL = 'M'
tipo_cambio = (CAMBIO_AUTOMATICO, CAMBIO_MANUAL, NullValue)

CARRO_NOVO = 'N'
CARRO_SEMINOVO = 'S'
tipo_carro_novo_seminovo = (CARRO_NOVO, CARRO_SEMINOVO, NullValue)

class Anuncio(BaseDbTable):
    id = IntegerPrimaryKey()
    titulo = VarcharField(db_name='titulo', size=255, nullable=False)
    descricao = VarcharField(db_name='descricao', size=1000)
    modelo = ForeignKey(ModeloVeiculo, index=True)
    ano_fabricacao = IntegerField(db_name='ano_fabricacao')
    ano_modelo = IntegerField(db_name='ano_modelo')
    quilometragem = IntegerField(db_name='quilometragem')
    valor = NumericField(db_name='valor', scale=2, precision=20)
    cambio = CharacterField(db_name='cambio', size=1, permitted_values=tipo_cambio)
    cidade = ForeignKey(Cidade)
    motorizacao = VarcharField(db_name='motorizacao', size=50)
    novo_seminovo = CharacterField(db_name="novo_seminovo", size=1, permitted_values=tipo_carro_novo_seminovo)
    data_de_inclusao = DateField(db_name='data_inclusao', index=True, nullable=False, default=CurrentDate)
