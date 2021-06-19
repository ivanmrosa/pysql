import unittest, os
from core.pysql_config import DB_DRIVER
from core.sql_db_tables import BaseDbTable
from core.sql_operators import *
from core.pysql_command import select, insert, update, delete
from core.pysql_functions import fsum, favg, fcount, fmax, fmin, fupper, flower, fsubstr, ftrim, fltrim, frtrim, flength, freplace, finstr, \
    fconcat, fdistinct, fconcat, frpad, flpad
from test.models.cidade import Cidade
from test.models.estado import Estado
from test.models.pais import Pais
from test.models.modelo_fipe import ModeloFipe
from test.models.modelo_veiculo import ModeloVeiculo
from test.models.mercadoria import Produto, Venda, VendaMultipla
from test.models.fonte_de_anuncio import FonteDeAnuncio
from setup.pysql_setup import manage_db
from core.db_types import NullValue
from core.unit_of_work import UnitOFWork
import datetime



def recreate_db():
    base = os.path.join(os.getcwd(), 'test/') 
    manage_db(clear_cache_param='RECREATEDB', ask_question=False, base_dir= base, models_package='test.models')


class TestDbTables(unittest.TestCase):
    def test_get_table_alias_default(self):
       self.assertEqual(Pais.get_alias().upper(), 'PAIS')
    
    def test_get_table_alias_changing_alias(self):
        Pais2 = Pais.get_copy('Pais2')
        self.assertEqual(Pais2.get_alias().upper(), 'PAIS2')
        self.assertEqual(Pais.get_alias().upper(), 'PAIS')
        #self.test_get_table_alias_default()
    
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
            pass
            #raise Exception('Driver ' + DB_DRIVER + ' not implemented')
    
    def test_get_create_pk(self):
        if DB_DRIVER == 'POSTGRESQL':
            script = 'ALTER TABLE PAIS ADD CONSTRAINT PK_PAIS PRIMARY KEY(ID)'
            self.assertEqual(Pais.get_script_create_pk()[1].upper(), script)
        else:
            #raise Exception('Driver ' + DB_DRIVER + ' not implemented')
            pass
    
    def test_get_create_index(self):
        if DB_DRIVER == 'POSTGRESQL':            
            self.assertEqual(Pais.get_scripts_indices()[0][1].upper(), 'CREATE UNIQUE INDEX UX_PAIS_NOME ON PAIS(NOME)')
            self.assertEqual(Pais.get_scripts_indices()[1][1].upper(), 'CREATE INDEX IX_PAIS_CODIGO ON PAIS(CODIGO)')
            self.assertEqual(ModeloFipe.get_scripts_indices()[1][1].upper(), 'CREATE INDEX IX_MODELO_FIPE_MARCA_NOME ON MODELO_FIPE(MARCA_FIPE_ID,NOME)') 
        else:
            #raise Exception('Driver ' + DB_DRIVER + ' not implemented')
            pass
    
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

@unittest.skipIf(DB_DRIVER != 'POSTGRESQL', 'NOT POSTGRE') 
class TestOperators(unittest.TestCase):
    
    def test_sql_simple_select(self):        
        sql = select(Estado).get_sql()
        self.assertEqual(sql.upper(), 'SELECT ESTADO.ID, ESTADO.PAIS_ID AS PAIS, ESTADO.NOME, ESTADO.SIGLA FROM ESTADO ESTADO')
    
    def test_sql_simple_select_with_fields(self):
        sql = select(Estado).get_sql(Estado.nome)
        self.assertEqual(sql.upper(), 'SELECT ESTADO.NOME FROM ESTADO ESTADO')
        sql = select(Estado).get_sql(Estado.nome, Estado.pais)
        self.assertEqual(sql.upper(), 'SELECT ESTADO.NOME, ESTADO.PAIS_ID AS PAIS FROM ESTADO ESTADO')

    def test_sql_select_with_join(self):
        sql = select(Estado).join(Pais).get_sql()
        sql_test = 'SELECT ESTADO.ID, ESTADO.PAIS_ID AS PAIS, ESTADO.NOME, ESTADO.SIGLA, PAIS.ID, PAIS.NOME, PAIS.CODIGO FROM ESTADO ESTADO JOIN PAIS PAIS ON ESTADO.PAIS_ID = PAIS.ID'
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
        sql_test = 'SELECT PAIS.NOME, ESTADO.NOME, CIDADE.NOME, CIDADE.ESTADO_ID AS ESTADO '+ \
        'FROM CIDADE CIDADE '+ \
        'JOIN ESTADO ESTADO ON CIDADE.ESTADO_ID = ESTADO.ID '+ \
        'JOIN PAIS PAIS ON ESTADO.PAIS_ID = PAIS.ID'
        self.assertEqual(sql.upper(), sql_test)

    def test_sql_with_field_alias(self):
        sql = select(Cidade).join(Estado).join(Pais).\
            get_sql((Pais.nome, 'NOME_PAIS'), (Estado.nome, 'NOME_ESTADO'), \
                (Cidade.nome, 'NOME_CIDADE'), Cidade.estado)
        sql_test = 'SELECT PAIS.NOME AS NOME_PAIS, ESTADO.NOME AS NOME_ESTADO, CIDADE.NOME AS NOME_CIDADE, CIDADE.ESTADO_ID AS ESTADO '+ \
        'FROM CIDADE CIDADE '+ \
        'JOIN ESTADO ESTADO ON CIDADE.ESTADO_ID = ESTADO.ID '+ \
        'JOIN PAIS PAIS ON ESTADO.PAIS_ID = PAIS.ID'
        self.assertEqual(sql.upper(), sql_test)

    def test_sql_get_all_fields_from_one_table(self):
        sql = select(Cidade).join(Estado).join(Pais).\
            get_sql(Estado, Cidade.nome, (Pais.codigo, 'CODIGO_PAIS'))
        sql_test = 'SELECT ESTADO.*, CIDADE.NOME, PAIS.CODIGO AS CODIGO_PAIS '+ \
        'FROM CIDADE CIDADE '+ \
        'JOIN ESTADO ESTADO ON CIDADE.ESTADO_ID = ESTADO.ID '+ \
        'JOIN PAIS PAIS ON ESTADO.PAIS_ID = PAIS.ID'
        self.assertEqual(sql.upper(), sql_test)
    
    def test_sql_where(self):
        self.maxDiff = None
        sql = select(Cidade).join(Estado).join(Pais).filter(oequ(Cidade.nome, 'BELO HORIZONTE') , 
            odif(Estado.nome, 'BAHIA') ).filter(oequ(Pais.codigo, 50)). \
            get_sql(Estado, Cidade.nome, (Pais.codigo, 'CODIGO_PAIS'))
        sql_test = 'SELECT ESTADO.*, CIDADE.NOME, PAIS.CODIGO AS CODIGO_PAIS '+ \
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
        sql_test = 'SELECT ESTADO.*, CIDADE.NOME, PAIS.CODIGO AS CODIGO_PAIS '+ \
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
        #base = os.path.join(os.getcwd(), 'test/') 
        #manage_db(clear_cache_param='RECREATEDB', ask_question=False, base_dir= base, models_package='test.models')
        recreate_db()
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
        Estado.sigla.value = 'MG'
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


        Pais.clear()
        Pais.nome.value = 'Argentina'
        insert(Pais).run()

        Cidade.clear()
        Cidade.nome.value = 'Belo Horizonte'
        Cidade.estado.value = select(Estado).filter(oequ(Estado.sigla, 'MG')).values(Estado.id).get_first()["id"]
        insert(Cidade).run() 
    
       
    def test_simple_sql(self):
        paises = select(Pais).values()
        self.assertEqual(paises[0][1], 'Brasil')
    
    def test_sql_with_fk_field(self):
        self.assertNotEqual(len(select(Cidade).values(Cidade.estado, Cidade.nome)), 0)

    
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
        sql_obj = select(Estado).filter(
            olike(Estado.nome, 'Ca%')
        )
        
        estados = sql_obj.values(Estado.nome)     
        self.assertGreater(len(estados), 0)   
        for estado in estados:            
            self.assertIn(estado[0], ('California', 'Carolina do Norte'))        
    
    def test_sql_filter_not_like(self):
        sql_obj = select(Estado).filter(
            onlike(Estado.nome, 'Ca%')
        )
        
        estados = sql_obj.values(Estado.nome)  
        self.assertGreater(len(estados), 0)         
        for estado in estados:            
            self.assertNotIn(estado[0], ('California', 'Carolina do Norte'))        

    def test_sql_filter_is_null(self):
        paises = select(Pais).filter(onull(Pais.codigo)).values(Pais.nome)
        self.assertGreater(len(paises), 0)   
        for pais in paises:
            self.assertEqual(pais[0], 'Argentina')

    def test_sql_filter_is_not_null(self):
        data = select(Pais).filter(onnull(Pais.codigo))
        paises = data.values(Pais.nome)
        self.assertGreater(len(paises), 0)   
        for pais in paises:
            self.assertIsNot(pais[0], 'Argentina')
    
    def test_sql_filter_bigger_than(self):
        paises = select(Pais).filter(obt(Pais.codigo, '0055')).values(Pais.nome)
        self.assertGreater(len(paises), 0)   
        for pais in paises:
            self.assertIn(pais[0], 'Estados Unidos Da América', 'Argentina')

    def test_sql_filter_less_than(self):
        paises = select(Pais).filter(olt(Pais.codigo, '0056')).values(Pais.nome)
        self.assertGreater(len(paises), 0)   
        for pais in paises:
            self.assertEqual(pais[0], 'Brasil')
    
    def test_sql_filter_exists(self):
        pais_filter = select(Pais).filter(oequ(Pais.codigo, '0055'), oequ(Estado.pais, Pais.id))                 
        estados = select(Estado).filter(oex(pais_filter)).values(Estado.nome)
        self.assertGreater(len(estados), 0)   
         
        for estado in estados:            
            self.assertIn(estado[0], ('Minas Gerais', 'São Paulo'))

    def test_sql_filter_not_exists(self):
        pais_filter = select(Pais).filter(oequ(Pais.codigo, '0055'), oequ(Estado.pais, Pais.id))
        estados = select(Estado).filter(onex(pais_filter)).values(Estado.nome)
        self.assertGreater(len(estados), 0)   

        for estado in estados:            
            self.assertNotIn(estado[0], ('Minas Gerais', 'São Paulo'))
    
    def test_simple_update(self):
        filtro_estado = (oequ(Estado.nome, 'Minas Gerais'),)
        Estado.clear()
        Estado.sigla.value = 'MG'
        update(Estado).filter(*filtro_estado).run()
        estados = select(Estado).filter(*filtro_estado).values(Estado.sigla)
        self.assertGreater(len(estados), 0)   

        for estado in estados:            
            self.assertEqual(estado[0], 'MG')

    def test_update_using_fields(self):
        Estado.clear()
        Estado.nome.value = Estado.sigla
        update(Estado).filter(oequ(Estado.nome, 'Minas Gerais')).run()
        self.assertEqual(len(select(Estado).filter(oequ(Estado.nome, 'Minas Gerais')).values()), 0)
        self.assertEqual(len(select(Estado).filter(oequ(Estado.nome, 'MG')).values()), 1)

        
    def test_from_update(self):
        filtro_estado = (oequ(Pais.nome, 'Brasil'),)
        Estado.clear()
        Estado.sigla.value = 'BR'
        update(Estado).join(Pais).filter(*filtro_estado).run()
        estados = select(Estado).join(Pais).filter(*filtro_estado).values(Estado.sigla)
        self.assertGreater(len(estados), 0)   

        for estado in estados:            
            self.assertEqual(estado[0], 'BR')
    
    def test_friendly_data_dict(self):
        estados = select(Estado).join(Pais).filter(oequ(Estado.nome, 'Minas Gerais')).values((Estado.nome, 'estado_nome'), (Pais.nome, 'pais_nome'))
        self.assertEqual(estados[0]['estado_nome'], 'Minas Gerais')
        self.assertEqual(estados[0]['pais_nome'], 'Brasil')
    
    def test_simple_delete(self):
        filtro = ( oequ(Pais.nome, 'Argentina'), )
        self.assertEqual(len(select(Pais).filter(*filtro).values(Pais.nome)), 1)
        delete(Pais).filter(*filtro).run()
        self.assertEqual(len(select(Pais).filter(*filtro).values(Pais.nome)), 0)
        self.assertEqual(len(select(Pais).filter(oequ(Pais.nome, 'Brasil')).values(Pais.nome)), 1)

    def test_delete_with_from(self):
        filtro = ( oequ(Pais.nome, 'Brasil'), )
        self.assertEqual(len(select(Estado).join(Pais).filter(*filtro).values(Pais.nome)), 2)
        delete(Cidade).run()
        delete(Estado).join(Pais).filter(*filtro).run()
        self.assertEqual(len(select(Estado).join(Pais).filter(*filtro).values(Pais.nome)), 0)
        self.assertEqual(len(select(Estado).join(Pais).filter(oequ(Pais.nome, 'Estados Unidos Da América')).values(Pais.nome)), 3)

class TestManyToManyField(unittest.TestCase):
    def setUp(self):
        recreate_db()
        Produto.clear()
        Produto.clear()
        Produto.nome.value = 'Pneu aro 15'
        Produto.categoria.value = 'PNEU'
        Produto.valor_unitario.value = 350.50
        insert(Produto).run()

        Produto.clear()
        Produto.nome.value = 'Pneu aro 13'
        Produto.categoria.value = 'PNEU'
        Produto.valor_unitario.value = 199.99
        insert(Produto).run()

        Produto.clear()
        Produto.nome.value = 'Roda de aço aro 13'
        Produto.categoria.value = 'RODA'
        Produto.valor_unitario.value = 540
        insert(Produto).run()

        Produto.clear()
        Produto.nome.value = 'Roda de aço aro 15'
        Produto.categoria.value = 'RODA'
        Produto.valor_unitario.value = 950
        insert(Produto).run()
        
        Produto.clear()
        Produto.nome.value = ' Limpador de parabrisa '
        Produto.categoria.value = 'ZZZZ'
        Produto.valor_unitario.value = 0
        insert(Produto).run()

    def test_insert(self):
        get_id_produto = lambda nome_prod : select(Produto).filter(oequ(Produto.nome, nome_prod)).values(Produto.id)[0]['id']
        VendaMultipla.clear()
        VendaMultipla.data_venda.value = datetime.datetime.now()
        VendaMultipla.produtos.add(get_id_produto('Roda de aço aro 13'))
        VendaMultipla.produtos.add(get_id_produto('Pneu aro 13'))
        insert(VendaMultipla).run()

        vendas_produto = select(VendaMultipla).join(Produto).values(Produto.nome)
        
        self.assertEqual(len(vendas_produto), 2)

        VendaMultipla.clear()
        VendaMultipla.id.value = select(VendaMultipla).values(fmax(VendaMultipla.id)).get_first()["id"]
        VendaMultipla.produtos.add(get_id_produto(' Limpador de parabrisa '))
        insert(VendaMultipla.produtos).run()

        vendas_produto = select(VendaMultipla).join(Produto).values(Produto.nome)        
        self.assertEqual(len(vendas_produto), 3)

    def test_delete(self):
        get_id_produto = lambda nome_prod : select(Produto).filter(oequ(Produto.nome, nome_prod)).values(Produto.id)[0]['id']
        VendaMultipla.clear()
        VendaMultipla.data_venda.value = datetime.datetime.now()
        VendaMultipla.produtos.add(get_id_produto('Roda de aço aro 13'))
        VendaMultipla.produtos.add(get_id_produto('Pneu aro 13'))
        insert(VendaMultipla).run()

        delete(VendaMultipla.produtos).filter(oequ(Produto.nome, 'Roda de aço aro 13')).run()

        vendas_produto = select(VendaMultipla).join(Produto).values(Produto.nome)
        
        self.assertEqual(len(vendas_produto), 1)
        
        vendas_produto = select(VendaMultipla).join(Produto).values(Produto.nome)
        self.assertEqual(vendas_produto[0]["nome"], 'Pneu aro 13')
        
class TestTableDbNameDifferentFromClassName(unittest.TestCase):
    def setUp(self):
        recreate_db()
        FonteDeAnuncio.clear()
        FonteDeAnuncio.nome.value = 'teste'
        FonteDeAnuncio.url_pagina_pricipal.value = 'teste@teste.com'
        insert(FonteDeAnuncio).run()
    
    def test_select(self):
        nome = select(FonteDeAnuncio).values(FonteDeAnuncio.nome).get_first()["nome"]
        self.assertEqual(nome, 'teste')

class TestStandardFunctions(unittest.TestCase):
    def setUp(self):
        recreate_db()
        Produto.clear()
        Produto.nome.value = 'Pneu aro 15'
        Produto.categoria.value = 'PNEU'
        Produto.valor_unitario.value = 350.50
        insert(Produto).run()

        Produto.clear()
        Produto.nome.value = 'Pneu aro 13'
        Produto.categoria.value = 'PNEU'
        Produto.valor_unitario.value = 199.99
        insert(Produto).run()

        Produto.clear()
        Produto.nome.value = 'Roda de aço aro 13'
        Produto.categoria.value = 'RODA'
        Produto.valor_unitario.value = 540
        insert(Produto).run()

        Produto.clear()
        Produto.nome.value = 'Roda de aço aro 15'
        Produto.categoria.value = 'RODA'
        Produto.valor_unitario.value = 950
        insert(Produto).run()
        
        Produto.clear()
        Produto.nome.value = ' Limpador de parabrisa '
        Produto.categoria.value = 'ZZZZ'
        Produto.valor_unitario.value = 0
        insert(Produto).run()

        get_id_produto = lambda nome_prod : select(Produto).filter(oequ(Produto.nome, nome_prod)).values(Produto.id)[0]['id']
        
        Venda.clear()
        Venda.produto.value = get_id_produto('Pneu aro 15')
        Venda.quantidade.value = 4
        insert(Venda).run()
        
        Venda.clear()
        Venda.produto.value = get_id_produto('Roda de aço aro 15')
        Venda.quantidade.value = 4
        insert(Venda).run()

        Venda.clear()
        Venda.produto.value = get_id_produto('Pneu aro 13')
        Venda.quantidade.value = 2
        insert(Venda).run()
        
        Venda.clear()
        Venda.produto.value = get_id_produto('Roda de aço aro 13')
        #Venda.quantidade.value = NullValue
        insert(Venda).run()

    def test_sum_simple_select(self):
        vendas = select(Venda).join(Produto).filter(olike(Produto.nome, 'Pneu%')).values(fsum(Produto.valor_unitario, 'soma_valor_unitario'))[0]
        self.assertEqual(float(vendas['soma_valor_unitario']), 550.49)

        vendas = select(Venda).join(Produto).filter(olike(Produto.nome, 'Pneu%')).values(fsum(Produto.valor_unitario))[0]
        self.assertEqual(float(vendas['valor_unitario']), 550.49)
    
    def test_sum_select_group_by(self):
        vendas = select(Venda).join(Produto).values(fsum(Produto.valor_unitario, 'soma_valor_unitario'), fsum(Venda.quantidade), Produto.categoria)
        for venda in vendas:
            if venda['categoria'] == 'PNEU':
                self.assertEqual(float(venda['soma_valor_unitario']), 550.49)
                self.assertEqual(float(venda['quantidade']), 6)
            elif venda['categoria'] == 'RODA':
                self.assertEqual(float(venda['soma_valor_unitario']), 1490)
                self.assertEqual(float(venda['quantidade']), 4)
            else:
                raise Exception('CATEGORIA NÃO PERTENCENTE AO FILTRO ' + venda['categoria'])
    
    def test_avg(self):
        vendas = select(Venda).join(Produto).values(favg(Produto.valor_unitario), Produto.categoria)
        for venda in vendas:
            if venda['categoria'] == 'PNEU':
                self.assertEqual(float(venda['valor_unitario']), 275.245)
            elif venda['categoria'] == 'RODA':
                self.assertEqual(float(venda['valor_unitario']), 745)
            else:
                raise Exception('CATEGORIA NÃO PERTENCENTE AO FILTRO ' + venda['categoria'])

    def test_distinct(self):
        vendas = select(Venda).join(Produto).values(fdistinct(Produto.categoria))
        self.assertEqual(len(vendas), 2)

        vendas = select(Venda).join(Produto).values(fdistinct(Produto.categoria, Venda.produto))
        self.assertEqual(len(vendas), 4)

    def test_count(self):
        vendas = select(Venda).values(fcount())
        self.assertEqual(vendas[0][0], 4)

        vendas = select(Venda).join(Produto).values(fcount(fdistinct(Produto.categoria)))
        self.assertEqual(vendas[0][0], 2)
     
        vendas = select(Venda).values(fcount(Venda.quantidade))
        self.assertEqual(vendas[0][0], 3)

        vendas = select(Venda).join(Produto).values(fcount(fdistinct(Produto.categoria)), Produto.categoria)
        self.assertEqual(vendas[0][0], 1)
        self.assertEqual(vendas[0][0], 1)
    
    def test_max(self):
        self.assertEqual(select(Produto).values(fmax(Produto.valor_unitario))[0]['valor_unitario'], 950)
        vendas = select(Produto).order_by(Produto.categoria).values(fmax(Produto.valor_unitario), Produto.categoria)
        self.assertEqual(float(vendas[0]['valor_unitario']), 350.50)
        self.assertEqual(float(vendas[1]['valor_unitario']), 950)
        
        vendas = select(Produto).order_by(Produto.categoria).values(fmax(Produto.valor_unitario))
        self.assertEqual(float(vendas[0]['valor_unitario']), 350.50)
        self.assertEqual(float(vendas[1]['valor_unitario']), 950.00)

    def test_min(self):
        self.assertEqual(float(select(Produto).filter(onlike(Produto.nome, "%Limpador%")).values(fmin(Produto.valor_unitario))[0]['valor_unitario']), 199.99)
        vendas = select(Produto).order_by(Produto.categoria).values(fmin(Produto.valor_unitario), Produto.categoria)
        self.assertEqual(float(vendas[0]['valor_unitario']), 199.99)
        self.assertEqual(float(vendas[1]['valor_unitario']), 540)
        
        vendas = select(Produto).order_by(Produto.categoria).values(fmin(Produto.valor_unitario))
        self.assertEqual(float(vendas[0]['valor_unitario']), 199.99)
        self.assertEqual(float(vendas[1]['valor_unitario']), 540.00)
    
    def test_upper(self):
        produto_nome = select(Produto).filter(oequ(Produto.nome, 'Pneu aro 13')).values(fupper(Produto.nome))[0]['nome']
        self.assertEqual(produto_nome, 'PNEU ARO 13')

        produto_nome = select(Produto).filter(onlike(Produto.nome, "%Limpador%")).order_by(fupper(Produto.nome)).values(fupper(Produto.nome))[0]['nome']
        self.assertEqual(produto_nome, 'PNEU ARO 13')

    def test_lower(self):
        produto_nome = select(Produto).filter(oequ(Produto.nome, 'Pneu aro 13')).values(flower(Produto.nome))[0]['nome']
        self.assertEqual(produto_nome, 'pneu aro 13')

        produto_nome = select(Produto).filter(onlike(Produto.nome, "%Limpador%")).order_by(flower(Produto.nome)).values(flower(Produto.nome))[0]['nome']
        self.assertEqual(produto_nome, 'pneu aro 13')
    
    def test_substr(self):    
        produto_nome = select(Produto).filter(oequ(Produto.categoria, 'PNEU')).\
            order_by(fsubstr(Produto.nome, 10, 2)).values(fsubstr(Produto.nome, 10, 2))[0]['nome']
        self.assertEqual(produto_nome, '13')
    
    def test_trim(self):
        self.assertEqual(select(Produto).filter(oequ(Produto.nome, 'Pneu aro 15')).values(ftrim(Produto.nome, 'P'))[0]['nome'], 'neu aro 15')
        self.assertEqual(select(Produto).filter(oequ(Produto.nome, ' Limpador de parabrisa ')).values(ftrim(Produto.nome))[0]['nome'], 'Limpador de parabrisa')
        

    def test_ltrim(self):
        self.assertEqual(select(Produto).filter(oequ(Produto.nome, 'Pneu aro 15')).values(fltrim(Produto.nome, 'P'))[0]['nome'], 'neu aro 15')        
        self.assertEqual(select(Produto).filter(oequ(Produto.nome, ' Limpador de parabrisa ')).values(fltrim(Produto.nome))[0]['nome'], 'Limpador de parabrisa ')

    def test_rtrim(self):
        self.assertEqual(select(Produto).filter(oequ(Produto.nome, 'Pneu aro 15')).values(frtrim(Produto.nome, '5'))[0]['nome'], 'Pneu aro 1')
        self.assertEqual(select(Produto).filter(oequ(Produto.nome, ' Limpador de parabrisa ')).values(frtrim(Produto.nome))[0]['nome'], ' Limpador de parabrisa')
    
    def test_length(self):
        self.assertEqual(select(Produto).filter(oequ(Produto.nome, ' Limpador de parabrisa ')).values(flength(Produto.nome))[0]['nome'], 23)
        self.assertEqual(select(Produto).filter(oequ(Produto.nome, ' Limpador de parabrisa ')).values(flength(ftrim(Produto.nome)))[0]['nome'], 21)
    
    def test_replace(self):
        self.assertEqual(select(Produto).filter(oequ(Produto.nome, ' Limpador de parabrisa ')).values(freplace(Produto.nome, 'p', 'b'))[0]['nome'], \
            ' Limbador de barabrisa ')
    
    def test_instr(self):
        self.assertEqual(select(Produto).filter(oequ(Produto.nome, ' Limpador de parabrisa ')).values(finstr(Produto.nome, 'L'))[0]['nome'], 2)

    def test_rpad(self):
        self.assertEqual(select(Produto).filter(oequ(Produto.nome, 'Pneu aro 15')).values(frpad(Produto.nome, '0', 13))[0]['nome'], 'Pneu aro 1500')

    def test_lpad(self):
        self.assertEqual(select(Produto).filter(oequ(Produto.nome, 'Pneu aro 15')).values(flpad(Produto.nome, '0', 13))[0]['nome'], '00Pneu aro 15')
    
    #functions used in filter
    def test_upper_filter(self):
        produto_nome = select(Produto).filter(oequ(fupper(Produto.nome), 'PNEU ARO 13')).values(fupper(Produto.nome))[0]['nome']
        self.assertEqual(produto_nome, 'PNEU ARO 13')

        produto_nome = select(Produto).filter(onlike(fupper(Produto.nome), "%LIMPADOR%")).order_by(fupper(Produto.nome)).values(fupper(Produto.nome))[0]['nome']
        self.assertEqual(produto_nome, 'PNEU ARO 13')

    def test_lower_filter(self):
        produto_nome = select(Produto).filter(oequ(flower(Produto.nome), 'pneu aro 13')).values(flower(Produto.nome))[0]['nome']
        self.assertEqual(produto_nome, 'pneu aro 13')

        produto_nome = select(Produto).filter(onlike(flower(Produto.nome), "%limpador%")).order_by(flower(Produto.nome)).values(flower(Produto.nome))[0]['nome']
        self.assertEqual(produto_nome, 'pneu aro 13')
    
    def test_substr_filter(self):    
        produto_nome = select(Produto).filter(oequ(Produto.categoria, 'PNEU'), 
            oequ(fsubstr(Produto.nome, 10, 2), '13')).\
            order_by(fsubstr(Produto.nome, 10, 2)).values(fsubstr(Produto.nome, 10, 2))[0]['nome']
        self.assertEqual(produto_nome, '13')
    
    def test_trim_filter(self):
        self.assertEqual(select(Produto).filter(oequ(ftrim(Produto.nome, 'P'), 'neu aro 15')).values(ftrim(Produto.nome, 'P'))[0]['nome'], 'neu aro 15')
        self.assertEqual(select(Produto).filter(oequ(ftrim(Produto.nome), 'Limpador de parabrisa')).\
            values(ftrim(Produto.nome))[0]['nome'], 'Limpador de parabrisa')
        
    def test_ltrim_filter(self):
        self.assertEqual(select(Produto).filter(oequ(fltrim(Produto.nome, 'P'), 'neu aro 15')).\
            values(fltrim(Produto.nome, 'P'))[0]['nome'], 'neu aro 15')        
        self.assertEqual(select(Produto).filter(oequ(fltrim(Produto.nome), 'Limpador de parabrisa ')).\
            values(fltrim(Produto.nome))[0]['nome'], 'Limpador de parabrisa ')

    def test_rtrim_filter(self):
        self.assertEqual(select(Produto).filter(oequ(frtrim(Produto.nome, '5'), 'Pneu aro 1')).\
            values(frtrim(Produto.nome, '5'))[0]['nome'], 'Pneu aro 1')
        self.assertEqual(select(Produto).filter(oequ(frtrim(Produto.nome), ' Limpador de parabrisa')).\
            values(frtrim(Produto.nome))[0]['nome'], ' Limpador de parabrisa')
    
    def test_length_filter(self):
        self.assertEqual(select(Produto).filter(
            oequ(Produto.nome, ' Limpador de parabrisa '), \
            oequ(flength(Produto.nome), 23) ). \
            values(flength(Produto.nome))[0]['nome'], 23)
        
        self.assertEqual(select(Produto).filter(
                oequ(Produto.nome, ' Limpador de parabrisa '),
                oequ(flength(ftrim(Produto.nome)), 21) \
            ).values(flength(ftrim(Produto.nome)))[0]['nome'], 21)
    
    def test_replace_filter(self):
        self.assertEqual(select(Produto).filter(oequ(freplace(Produto.nome, 'p', 'b'), \
            ' Limbador de barabrisa ')).values(freplace(Produto.nome, 'p', 'b'))[0]['nome'], \
            ' Limbador de barabrisa ')
    
    def test_instr_filter(self):
        self.assertEqual(select(Produto).filter(
            oequ(Produto.nome, ' Limpador de parabrisa '),
            oequ(finstr(Produto.nome, 'L'), 2)
        ).values(finstr(Produto.nome, 'L'))[0]['nome'], 2)

    def test_rpad_filter(self):
        self.assertEqual(select(Produto).filter(oequ(frpad(Produto.nome, '0', 13), 'Pneu aro 1500')).values(frpad(Produto.nome, '0', 13))[0]['nome'], 'Pneu aro 1500')

    def test_lpad_filter(self):
        self.assertEqual(select(Produto).filter(oequ(flpad(Produto.nome, '0', 13), '00Pneu aro 15')).values(flpad(Produto.nome, '0', 13))[0]['nome'], '00Pneu aro 15')

class TestTransaction(unittest.TestCase):
    def setUp(self):
        #base = os.path.join(os.getcwd(), 'test/') 
        #manage_db(clear_cache_param='RECREATEDB', ask_question=False, base_dir= base, models_package='test.models')
        recreate_db()
    
    
    def test_rollback_insert(self):
        Pais.clear()
        Pais.nome.value = 'Brasil'
        Pais.codigo.value = '0055'
        pais_insert = insert(Pais)
        pais_insert.run()
        self.assertEqual(len(select(Pais).values(Pais.id)), 1)
        
        pais_id = select(Pais).values(Pais.id).get_first()["id"]
        
        Estado.clear()
        Estado.nome.value = 'São Paulo'
        Estado.pais.value = pais_id
        insert(Estado).run()
        self.assertEqual(len(select(Estado).values(Estado.id)), 1)
        
        UnitOFWork.discart()
        self.assertEqual(len(select(Pais).values(Pais.id)), 0)
        self.assertEqual(len(select(Estado).values(Estado.id)), 0)
    
    def test_commit_insert(self):
        Pais.clear()
        Pais.nome.value = 'Brasil'
        Pais.codigo.value = '0055'
        pais_insert = insert(Pais)
        pais_insert.run()
        self.assertEqual(len(select(Pais).values(Pais.id)), 1)
        
        pais_id = select(Pais).values(Pais.id).get_first()["id"]
        
        Estado.clear()
        Estado.nome.value = 'São Paulo'
        Estado.pais.value = pais_id
        insert(Estado).run()
        self.assertEqual(len(select(Estado).values(Estado.id)), 1)

        UnitOFWork.save()
        UnitOFWork.discart()
        self.assertEqual(len(select(Pais).values(Pais.id)), 1)
        self.assertEqual(len(select(Estado).values(Estado.id)), 1)

    def test_rollback_delete(self):
        Pais.clear()
        Pais.nome.value = 'Brasil'
        Pais.codigo.value = '0055'
        pais_insert = insert(Pais)
        pais_insert.run()
        self.assertEqual(len(select(Pais).values(Pais.id)), 1)
        UnitOFWork.save()

        delete(Pais).run()

        self.assertEqual(len(select(Pais).values(Pais.id)), 0)

        UnitOFWork.discart()
        self.assertEqual(len(select(Pais).values(Pais.id)), 1)
        
    def test_commit_delete(self):
        Pais.clear()
        Pais.nome.value = 'Brasil'
        Pais.codigo.value = '0055'
        pais_insert = insert(Pais)
        pais_insert.run()
        self.assertEqual(len(select(Pais).values(Pais.id)), 1)
        UnitOFWork.save()

        delete(Pais).run()

        self.assertEqual(len(select(Pais).values(Pais.id)), 0)
        
        UnitOFWork.save()
        self.assertEqual(len(select(Pais).values(Pais.id)), 0)

if __name__ == '__main__':
    unittest.main()