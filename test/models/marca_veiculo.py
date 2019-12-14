from sql_db_tables import BaseDbTable
from db_types import VarcharField, IntegerPrimaryKey, ForeignKey
from test.models.marca_fipe import MarcaFipe
from test.models.fonte_de_anuncio import FonteDeAnuncio

class MarcaVeiculo(BaseDbTable):
    id = IntegerPrimaryKey()
    nome = VarcharField(db_name='nome', size=50, unique=True, nullable=False)
    codigo_integracao = VarcharField(db_name='codigo_integracao', size=10)
    marca_fipe = ForeignKey(related_to_class=MarcaFipe, index=True)
    fonte_do_anuncio = ForeignKey(FonteDeAnuncio)

    @classmethod
    def get_db_name(cls):
        return 'marca_veiculo'
