import unittest, os
from pysql_config import DB_DRIVER
from sql_db_tables import BaseDbTable
from sql_operators import *
from pysql_command import select, insert
#from db_types import ForeignKey, IntegerField, VarcharField, MoneyField, CharacterField
from test.models.cidade import Cidade
from test.models.estado import Estado
from test.models.pais import Pais
from test.models.modelo_fipe import ModeloFipe
from pysql_setup import manage_db



class TestDbTables(unittest.TestCase):
    def test_get_table_alias_default(self):
       self.assertEqual(Pais.get_alias().upper(), 'PAIS')
    
    def test_get_table_alias_changing_alias(self):
        Pais2 = Pais.get_copy('Pais2')
        self.assertEqual(Pais2.get_alias().upper(), 'PAIS2')
        self.test_get_table_alias_default()
    
    def test_get_db_name(self):
        Pais3 = Pais.get_copy('PAIS3')
        self.assertEqual(Pais3.get_db_name().upper(), 'PAIS')
    
    def test_get_create_script(self):
        if DB_DRIVER == 'POSTGRESQL':
            script = 'CREATE TABLE PAIS(ID SERIAL NOT NULL, NOME VARCHAR(50) NOT NULL, CODIGO INTEGER)'
            self.assertEqual(Pais.get_script_create_table().upper(), script)
            script = 'CREATE TABLE ESTADO(ID SERIAL NOT NULL, PAIS_ID INTEGER NOT NULL, NOME VARCHAR(50) NOT NULL, SIGLA VARCHAR(2))'
            self.assertEqual(Estado.get_script_create_table().upper(), script)
        else:
            raise Exception('Driver ' + DB_DRIVER + ' not implemented')
    
    def test_get_create_pk(self):
        if DB_DRIVER == 'POSTGRESQL':
            script = 'ALTER TABLE PAIS ADD CONSTRAINT PK_PAIS PRIMARY KEY(ID)'
            self.assertEqual(Pais.get_script_create_pk()[1].upper(), script)
        else:
            raise Exception('Driver ' + DB_DRIVER + ' not implemented')
    
    def test_get_create_index(self):
        if DB_DRIVER == 'POSTGRESQL':            
            self.assertEqual(Pais.get_scripts_indices()[0][1].upper(), 'CREATE UNIQUE INDEX UX_PAIS_NOME ON PAIS(NOME)')
            self.assertEqual(Pais.get_scripts_indices()[1][1].upper(), 'CREATE INDEX IX_PAIS_CODIGO ON PAIS(CODIGO)')
            self.assertEqual(ModeloFipe.get_scripts_indices()[1][1].upper(), 'CREATE INDEX IX_MODELO_FIPE_MARCA_NOME ON MODELO_FIPE(MARCA_FIPE_ID,NOME)') 
        else:
            raise Exception('Driver ' + DB_DRIVER + ' not implemented')
    
    def test_get_create_foreign_keys(self):
        if DB_DRIVER == 'POSTGRESQL':            
            self.assertEqual(Estado.get_scripts_fk()[0][1].upper(), 'ALTER TABLE ESTADO ADD CONSTRAINT FK_EST_PAI_PAIS_ID'+
                ' FOREIGN KEY(PAIS_ID) REFERENCES PAIS(ID)')            
   
    def test_drop_table(self):
        if DB_DRIVER == 'POSTGRESQL':
            self.assertEqual(Estado.get_script_drop_table().upper(), 'DROP TABLE ESTADO')

class TestFields(unittest.TestCase):
    def test_get_field_owner(self):
        self.assertEqual(Pais.id.get_owner().get_db_name(), 'Pais')


class TestOperators(unittest.TestCase):
    def test_sql_simple_select(self):        
        sql = select(Estado).get_sql()
        self.assertEqual(sql.upper(), 'SELECT * FROM ESTADO ESTADO')
    
    def test_sql_simple_select_with_fields(self):
        sql = select(Estado).get_sql(Estado.nome)
        self.assertEqual(sql.upper(), 'SELECT ESTADO.NOME FROM ESTADO ESTADO')
        sql = select(Estado).get_sql(Estado.nome, Estado.pais)
        self.assertEqual(sql.upper(), 'SELECT ESTADO.NOME, ESTADO.PAIS_ID FROM ESTADO ESTADO')

    def test_sql_select_with_join(self):
        sql = select(Estado).join(Pais).get_sql()
        sql_test = 'SELECT * FROM ESTADO ESTADO JOIN PAIS PAIS ON ESTADO.PAIS_ID = PAIS.ID'
        self.assertEqual(sql.upper(), sql_test)

        sql = select(Estado).join(Pais).get_sql(Pais.nome, Estado.nome)
        sql_test = 'SELECT PAIS.NOME, ESTADO.NOME FROM ESTADO ESTADO JOIN PAIS PAIS ON ESTADO.PAIS_ID = PAIS.ID'
        self.assertEqual(sql.upper(), sql_test)

        sql = select(Cidade).join(Estado).join(Pais).get_sql(Pais.nome, Estado.nome, Cidade.nome)
        sql_test = 'SELECT PAIS.NOME, ESTADO.NOME, CIDADE.NOME '+ \
        'FROM CIDADE CIDADE '+ \
        'JOIN ESTADO ESTADO ON CIDADE.ESTADO_ID = ESTADO.ID '+ \
        'JOIN PAIS PAIS ON ESTADO.PAIS_ID = PAIS.ID'
        self.assertEqual(sql.upper(), sql_test)

        sql = select(Cidade).join(Estado).join(Pais).get_sql(Pais.nome, Estado.nome, Cidade.nome, Cidade.estado)
        sql_test = 'SELECT PAIS.NOME, ESTADO.NOME, CIDADE.NOME, CIDADE.ESTADO_ID '+ \
        'FROM CIDADE CIDADE '+ \
        'JOIN ESTADO ESTADO ON CIDADE.ESTADO_ID = ESTADO.ID '+ \
        'JOIN PAIS PAIS ON ESTADO.PAIS_ID = PAIS.ID'
        self.assertEqual(sql.upper(), sql_test)

    def test_sql_with_field_alias(self):
        sql = select(Cidade).join(Estado).join(Pais).\
            get_sql((Pais.nome, 'NOME_PAIS'), (Estado.nome, 'NOME_ESTADO'), \
                (Cidade.nome, 'NOME_CIDADE'), Cidade.estado)
        sql_test = 'SELECT PAIS.NOME NOME_PAIS, ESTADO.NOME NOME_ESTADO, CIDADE.NOME NOME_CIDADE, CIDADE.ESTADO_ID '+ \
        'FROM CIDADE CIDADE '+ \
        'JOIN ESTADO ESTADO ON CIDADE.ESTADO_ID = ESTADO.ID '+ \
        'JOIN PAIS PAIS ON ESTADO.PAIS_ID = PAIS.ID'
        self.assertEqual(sql.upper(), sql_test)

    def test_sql_get_all_fields_from_one_table(self):
        sql = select(Cidade).join(Estado).join(Pais).\
            get_sql(Estado, Cidade.nome, (Pais.codigo, 'CODIGO_PAIS'))
        sql_test = 'SELECT ESTADO.*, CIDADE.NOME, PAIS.CODIGO CODIGO_PAIS '+ \
        'FROM CIDADE CIDADE '+ \
        'JOIN ESTADO ESTADO ON CIDADE.ESTADO_ID = ESTADO.ID '+ \
        'JOIN PAIS PAIS ON ESTADO.PAIS_ID = PAIS.ID'
        self.assertEqual(sql.upper(), sql_test)
    
    def test_sql_where(self):
        self.maxDiff = None
        sql = select(Cidade).join(Estado).join(Pais).filter(oequ(Cidade.nome, 'BELO HORIZONTE') , 
            odif(Estado.nome, 'BAHIA') ).filter(oequ(Pais.codigo, 50)). \
            get_sql(Estado, Cidade.nome, (Pais.codigo, 'CODIGO_PAIS'))
        sql_test = 'SELECT ESTADO.*, CIDADE.NOME, PAIS.CODIGO CODIGO_PAIS '+ \
        'FROM CIDADE CIDADE '+ \
        'JOIN ESTADO ESTADO ON CIDADE.ESTADO_ID = ESTADO.ID '+ \
        'JOIN PAIS PAIS ON ESTADO.PAIS_ID = PAIS.ID '+\
        'WHERE ((CIDADE.NOME = %s) AND (ESTADO.NOME <> %s)) '+\
        'AND ((PAIS.CODIGO = %s))'
        self.assertEqual(sql.upper().replace('%S', '%s'), sql_test)

    def test_sql_where_or(self):
        self.maxDiff = None
        sql = select(Cidade).join(Estado).join(Pais).filter(oequ(Cidade.nome, 'BELO HORIZONTE') , 
            odif(Estado.nome, 'BAHIA'), oequ(Pais.codigo, 50), oor(oequ(Pais.codigo, 1) )).  \
            get_sql(Estado, Cidade.nome, (Pais.codigo, 'CODIGO_PAIS'))
        sql_test = 'SELECT ESTADO.*, CIDADE.NOME, PAIS.CODIGO CODIGO_PAIS '+ \
        'FROM CIDADE CIDADE '+ \
        'JOIN ESTADO ESTADO ON CIDADE.ESTADO_ID = ESTADO.ID '+ \
        'JOIN PAIS PAIS ON ESTADO.PAIS_ID = PAIS.ID '+\
        'WHERE ((CIDADE.NOME = %s) AND (ESTADO.NOME <> %s) '+\
        'AND (PAIS.CODIGO = %s) '+\
        'OR (PAIS.CODIGO = %s))'
        self.assertEqual(sql.upper().replace('%S', '%s'), sql_test)

    def test_simple_insert(self):
        self.maxDiff = None
        Pais.id.value = 1
        Pais.nome.value = 'Brasil'
        Pais.codigo.value = '0055'
        script = insert(Pais).get_script()
       
        script_text = 'INSERT INTO PAIS(ID, NOME, CODIGO) VALUES(%s, %s, %s)'
        self.assertEqual(script.upper().replace('%S', '%s'), script_text)
    
class TestExecutionOnDataBase(unittest.TestCase):
    
    def setUp(self):
        manage_db(clear_cache_param='RECREATEDB', ask_question=False, base_dir='/Users/Mac/Documents/dev/PYSQL/test/', models_package='test.models')
        Pais.clear()
        Pais.nome.value = 'Brasil'
        Pais.codigo.value = '0055'
        insert(Pais).run()

        pais_id = select(Pais).values(Pais.id)[0][0]
        Estado.clear()
        Estado.nome.value = 'São Paulo'
        Estado.pais.value = pais_id
        insert(Estado).run()


        Estado.clear()
        Estado.nome.value = 'Minas Gerais'
        Estado.pais.value = pais_id
        insert(Estado).run()

        Pais.clear()
        Pais.nome.value = 'Estados Unidos Da América'
        Pais.codigo.value = '0056'
        insert(Pais).run()

        pais_id = select(Pais).filter(oequ(Pais.nome, 'Estados Unidos Da América')).values(Pais.id)[0][0]
        Estado.clear()
        Estado.nome.value = 'California'
        Estado.pais.value = pais_id
        insert(Estado).run()

        Estado.clear()
        Estado.nome.value = 'Carolina do Norte'
        Estado.pais.value = pais_id
        insert(Estado).run()

        Estado.clear()
        Estado.nome.value = 'Texas'
        Estado.pais.value = pais_id
        insert(Estado).run()

    
       
    def test_simple_sql(self):
        paises = select(Pais).values()
        self.assertEqual(paises[0][1], 'Brasil')
    
    def test_sql_with_simple_join(self):

        estados = select(Estado).join(Pais).filter(oequ(Estado.nome, 'Minas Gerais')).values(Estado.nome, Pais.nome)
        self.assertEqual(estados[0][0], 'Minas Gerais')
        self.assertEqual(estados[0][1], 'Brasil')
    
    def test_sql_with_simple_filter(self):

        estados = select(Estado).join(Pais).filter(oequ(Pais.nome, 'Brasil'), 
            oequ(Estado.nome, 'São Paulo')).values(Estado.nome, Pais.nome)
        
        self.assertEqual(estados[0][0], 'São Paulo')
        self.assertEqual(estados[0][1], 'Brasil')
    
    def test_sql_filter_or(self):

        sql_obj = select(Estado).join(Pais).filter(
            oequ(Estado.nome, 'São Paulo'),
            oor(oequ(Estado.nome, 'Minas Gerais') ))        
        estados = sql_obj.values(Estado.nome, Pais.nome)        
        
        for estado in estados:            
            self.assertIn(estado[0], ('São Paulo', 'Minas Gerais'), )
            self.assertEqual(estado[1], 'Brasil')
        

    def test_sql_filter_or_2(self):

        sql_obj = select(Estado).join(Pais).filter(            
                oequ(Pais.nome, 'Estados Unidos Da América'),
                odif(Estado.nome, 'Carolina do Norte'),
                odif(Estado.nome, 'Texas'),
                oor(oequ(Estado.nome, 'Minas Gerais'),  oequ(Pais.nome, 'Brasil'))                  
        )        
        estados = sql_obj.values(Estado.nome, Pais.nome)        
        for estado in estados:            
            self.assertIn(estado[0], ('California', 'Minas Gerais'))
            if estado[0] == 'California':
                self.assertEqual(estado[1], 'Estados Unidos Da América')
            else:    
                self.assertEqual(estado[1], 'Brasil')
    
    def test_sql_filter_in_clause(self):
        sql_obj = select(Estado).filter(
            oin(Estado.nome, 'California', 'Minas Gerais')
        )
        estados = sql_obj.values(Estado.nome)        
        for estado in estados:            
            self.assertIn(estado[0], ('California', 'Minas Gerais'))
        
    def test_sql_filter_in_clause_2(self):
        sql_obj = select(Estado).join(Pais).filter(
            oin(Estado.nome, 'California', 'Minas Gerais'),
            oequ(Pais.nome, 'Brasil')
        )

        estados = sql_obj.values(Estado.nome, Pais.nome)        
        for estado in estados:            
            self.assertIn(estado[0], ('Minas Gerais'))
    
    def test_sql_filter_in_clause_sub_sql(self):
        filter_pais_in = select(Pais).filter(oequ(Pais.nome, 'Brasil')).set_fields(Pais.id)
        sql_obj = select(Estado).filter(
            oin(Estado.pais, filter_pais_in)
        )
        
        estados = sql_obj.values(Estado.nome)        
        for estado in estados:            
            self.assertIn(estado[0], ('Minas Gerais', 'São Paulo'))

    def test_sql_filter_not_in_clause_sub_sql(self):
        filter_pais_in = select(Pais).filter(oequ(Pais.nome, 'Brasil')).set_fields(Pais.id)
        sql_obj = select(Estado).filter(
            onin(Estado.pais, filter_pais_in)
        )
        
        estados = sql_obj.values(Estado.nome)        
        for estado in estados:            
            self.assertIn(estado[0], ('Texas', 'California', 'Carolina do Norte'))
    
    def test_sql_filter_like(self):
        pass


if __name__ == '__main__':
    unittest.main()