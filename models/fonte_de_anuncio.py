from sql_db_tables import BaseDbTable
from db_types import IntegerPrimaryKey, VarcharField

class FonteDeAnuncio(BaseDbTable):   
    id = IntegerPrimaryKey()
    nome = VarcharField(db_name='nome', size=200, unique=True, nullable=False)
    url_pagina_pricipal = VarcharField(db_name='url', size=200)

    @classmethod
    def get_db_name(cls):
        return 'fonte_de_anuncio'
