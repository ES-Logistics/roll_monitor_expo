"""
Service para executar queries na base de dados
"""
import psycopg2
from services.datalake_connection import get_db_config

class QueryService:
    
    def __init__(self):
        self.query1 = '''
select  mov.ds_movimento as proceso, 
                    mov.dt_previsao_saida as previsao_embarque,
                    loc.nm_localidade as porto_embarque,
                    desloc.nm_localidade as porto_destino,
					nav.nm_navio  as navio_embarque,
                    trc.dt_previsao_saida as previsao_embarque_transbordo,
                    loctra.nm_localidade as porto_transbordo,
                    navtra.nm_navio as navio_transbordo,
                    usr.ds_email as email_responsavel,
                    pes.nm_fantasia as armador,
                    pescli.nm_fantasia as cliente,
                    mot.ds_motivo as motivo_transferencia
            from bronze.skyline_es_ocs_movimento mov
            left join bronze.skyline_es_ocs_servico as srv
            on srv.cd_movimento = mov.cd_movimento
            left join bronze.skyline_es_pes_pessoa as pes
            on pes.cd_pessoa = srv.cd_fornecedor
            left join bronze.skyline_es_usr_usuario as usr
            on mov.cd_responsavel = usr.cd_usuario
            left join bronze.skyline_es_ocs_trecho as trc
            on trc.cd_movimento  = mov.cd_movimento
            left join bronze.skyline_es_loc_localidade as loctra
            on trc.cd_localidade = loctra.cd_localidade 
            left join bronze.skyline_es_tab_navio as navtra
            on navtra.cd_navio = trc.cd_navio
            left join bronze.skyline_es_loc_localidade as locfee
            on mov.cd_origem_feeder = locfee.cd_localidade 
            left join bronze.skyline_es_tab_navio as navfee
            on mov.cd_navio_feeder = navfee.cd_navio 
            left join bronze.skyline_es_loc_localidade as loc
            on mov.cd_origem = loc.cd_localidade 
            left join bronze.skyline_es_loc_localidade as desloc
            on mov.cd_destino = desloc.cd_localidade
            left join bronze.skyline_es_tab_navio as nav
            on nav.cd_navio =mov.cd_navio 
            left join bronze.skyline_es_pes_pessoa as pescli
            on pescli.cd_pessoa = mov.cd_intermediario
            left join bronze.skyline_es_tab_motivo as mot
            on mot.cd_motivo = mov.cd_motivo_transferencia 
            where mov.ds_movimento ilike 'EM%'
            and mov.dt_previsao_saida is not null
            and mov.dt_confirmacao_saida is null
            and mov.dt_fechamento is null 
            and trc.dt_confirmacao_saida is null
            and srv.cd_servico =1
        '''
    
    def connect_to_database(self):
        """Conecta ao banco PostgreSQL usando as configurações do datalake_connection"""
        config = get_db_config()
        try:
            conn = psycopg2.connect(**config)
            return conn
        except Exception as e:
            raise Exception(f"Erro ao conectar no banco: {e}")
    
    def execute_monitoring_query(self):
        """Executa a query principal e retorna os dados como dicionário"""
        conn = self.connect_to_database()
        
        try:
            # Usar cursor para execução direta
            cursor = conn.cursor()
            cursor.execute(self.query1)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            cursor.close()
            conn.close()
            
            # Converte dados para dicionário usando ID único como chave
            data_dict = {}
            
            for i, row in enumerate(rows):
                row_dict = dict(zip(columns, row))
                proceso = row_dict['proceso']
                
                # Cria um ID único mais estável baseado nos dados, não no índice
                # Usa campos que realmente diferenciam os registros
                porto_embarque = str(row_dict.get('porto_embarque', 'NULL'))
                navio_embarque = str(row_dict.get('navio_embarque', 'NULL'))
                porto_destino = str(row_dict.get('porto_destino', 'NULL'))
                porto_transbordo = str(row_dict.get('porto_transbordo', 'NULL'))
                navio_transbordo = str(row_dict.get('navio_transbordo', 'NULL'))
                
                # ID único baseado apenas no processo (único identificador que não muda)
                unique_id = proceso
                
                # Adiciona todas as colunas mantendo 'proceso' original
                data_dict[unique_id] = {}
                for col, value in row_dict.items():
                    data_dict[unique_id][col] = value
                
                # Adiciona colunas de controle vazias
                data_dict[unique_id]['OBS'] = ''
                data_dict[unique_id]['CHANGES_DETAIL'] = {}
            
            return {
                'success': True,
                'data': data_dict,
                'records_count': len(data_dict),
                'message': f"Query executada com sucesso: {len(data_dict)} registros"
            }
            
        except Exception as e:
            if conn:
                conn.close()
            return {
                'success': False,
                'data': {},
                'error': str(e),
                'message': f"Erro ao executar query: {e}"
            }
    
    def initialize_database_tables(self):
        """Inicializa as tabelas se ainda não existirem"""
        conn = self.connect_to_database()
        
        try:
            cursor = conn.cursor()
            
            # Lê e executa o script de criação das tabelas
            with open('create_tables.sql', 'r', encoding='utf-8') as f:
                sql_script = f.read()
            
            cursor.execute(sql_script)
            conn.commit()
            return {
                'success': True,
                'message': "Tabelas inicializadas com sucesso"
            }
            
        except Exception as e:
            conn.rollback()
            return {
                'success': False,
                'error': str(e),
                'message': f"Erro ao inicializar tabelas: {e}"
            }
        finally:
            cursor.close()
            conn.close()