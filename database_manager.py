import psycopg2
import json
from datetime import datetime
from services.datalake_connection import get_db_config

def save_snapshot_to_db(data_dict):
    """Salva o snapshot atual no banco, substituindo os dados anteriores"""
    conn = psycopg2.connect(**get_db_config())
    
    try:
        cursor = conn.cursor()
        
        # Truncate da tabela (limpa dados anteriores)
        cursor.execute("TRUNCATE TABLE bronze.roll_monitor_expo_snapshot")
        
        # Insere novos dados
        insert_query = """
            INSERT INTO bronze.roll_monitor_expo_snapshot 
            (unique_id, proceso, porto_embarque, navio_embarque, previsao_embarque, 
             previsao_embarque_feeder, navio_feeder, porto_feeder, 
             previsao_embarque_transbordo, porto_transbordo, navio_transbordo)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        for unique_id, record in data_dict.items():
            # Pula campos de controle
            if record.get('OBS') is not None or record.get('CHANGES_DETAIL') is not None:
                continue
                
            cursor.execute(insert_query, (
                unique_id,
                record.get('proceso'),
                record.get('porto_embarque'),
                record.get('navio_embarque'),
                record.get('previsao_embarque'),
                record.get('previsao_embarque_feeder'),
                record.get('navio_feeder'),
                record.get('porto_feeder'),
                record.get('previsao_embarque_transbordo'),
                record.get('porto_transbordo'),
                record.get('navio_transbordo')
            ))
        
        conn.commit()
        print(f"Snapshot salvo no banco: {len(data_dict)} registros")
        
    except Exception as e:
        print(f"Erro ao salvar snapshot: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def load_snapshot_from_db():
    """Carrega o snapshot atual do banco"""
    conn = psycopg2.connect(**get_db_config())
    
    try:
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT unique_id, proceso, porto_embarque, navio_embarque, previsao_embarque, 
                   previsao_embarque_feeder, navio_feeder, porto_feeder, 
                   previsao_embarque_transbordo, porto_transbordo, navio_transbordo
            FROM bronze.roll_monitor_expo_snapshot
        """)
        
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        
        data_dict = {}
        for row in rows:
            row_dict = dict(zip(columns, row))
            unique_id = row_dict.pop('unique_id')
            
            # Adiciona campos de controle
            row_dict['OBS'] = ''
            row_dict['CHANGES_DETAIL'] = {}
            
            data_dict[unique_id] = row_dict
        
        print(f"Snapshot carregado do banco: {len(data_dict)} registros")
        return data_dict
        
    except Exception as e:
        print(f"Erro ao carregar snapshot: {e}")
        return {}
    finally:
        cursor.close()
        conn.close()

def register_process_changes(proceso, changes_detail):
    """Registra ou atualiza as alterações de um processo na tabela de changes"""
    conn = psycopg2.connect(**get_db_config())
    
    try:
        cursor = conn.cursor()
        
        # Verifica se o processo já tem registro
        cursor.execute(
            "SELECT alteracoes FROM bronze.roll_monitor_expo_changes WHERE proceso = %s",
            (proceso,)
        )
        
        existing_record = cursor.fetchone()
        
        # Cria entrada de alteração com timestamp
        new_change = {
            'timestamp': datetime.now().isoformat(),
            'alteracoes': changes_detail
        }
        
        if existing_record:
            # Processo já existe - adiciona nova alteração ao JSON existente
            existing_alteracoes = existing_record[0]
            
            # Inicializa lista de alterações se não existir
            if 'historico_alteracoes' not in existing_alteracoes:
                existing_alteracoes['historico_alteracoes'] = []
            
            # Adiciona nova alteração
            existing_alteracoes['historico_alteracoes'].append(new_change)
            existing_alteracoes['ultima_alteracao'] = datetime.now().isoformat()
            
            # Atualiza registro existente e marca como PENDENTE
            cursor.execute("""
                UPDATE bronze.roll_monitor_expo_changes 
                SET alteracoes = %s, 
                    status_relatorio = 'PENDENTE',
                    updated_at = CURRENT_TIMESTAMP
                WHERE proceso = %s
            """, (json.dumps(existing_alteracoes), proceso))
            
        else:
            # Processo novo - cria primeiro registro
            new_alteracoes = {
                'processo': proceso,
                'primeira_deteccao': datetime.now().isoformat(),
                'ultima_alteracao': datetime.now().isoformat(),
                'historico_alteracoes': [new_change]
            }
            
            cursor.execute("""
                INSERT INTO bronze.roll_monitor_expo_changes (proceso, alteracoes, status_relatorio)
                VALUES (%s, %s, 'PENDENTE')
            """, (proceso, json.dumps(new_alteracoes)))
        
        conn.commit()
        print(f"Alterações registradas para processo: {proceso}")
        
    except Exception as e:
        print(f"Erro ao registrar alterações do processo {proceso}: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def get_pending_changes():
    """Retorna todos os processos com alterações pendentes de relatório"""
    conn = psycopg2.connect(**get_db_config())
    
    try:
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT proceso, alteracoes, created_at, updated_at
            FROM bronze.roll_monitor_expo_changes 
            WHERE status_relatorio = 'PENDENTE'
            ORDER BY updated_at DESC
        """)
        
        rows = cursor.fetchall()
        
        pending_changes = []
        for row in rows:
            pending_changes.append({
                'proceso': row[0],
                'alteracoes': row[1],
                'created_at': row[2],
                'updated_at': row[3]
            })
        
        return pending_changes
        
    except Exception as e:
        print(f"Erro ao buscar alterações pendentes: {e}")
        return []
    finally:
        cursor.close()
        conn.close()

def mark_changes_as_reported(processos_list):
    """Marca uma lista de processos como já incluídos no relatório"""
    conn = psycopg2.connect(**get_db_config())
    
    try:
        cursor = conn.cursor()
        
        # Atualiza status para ENVIADO
        cursor.execute("""
            UPDATE bronze.roll_monitor_expo_changes 
            SET status_relatorio = 'ENVIADO',
                updated_at = CURRENT_TIMESTAMP
            WHERE proceso = ANY(%s)
        """, (processos_list,))
        
        conn.commit()
        print(f"Marcados como enviados: {len(processos_list)} processos")
        
    except Exception as e:
        print(f"Erro ao marcar como enviado: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def init_database_tables():
    """Inicializa as tabelas se ainda não existirem"""
    conn = psycopg2.connect(**get_db_config())
    
    try:
        cursor = conn.cursor()
        
        # Lê e executa o script de criação das tabelas
        with open('create_tables.sql', 'r', encoding='utf-8') as f:
            sql_script = f.read()
        
        cursor.execute(sql_script)
        conn.commit()
        print("Tabelas inicializadas com sucesso")
        
    except Exception as e:
        print(f"Erro ao inicializar tabelas: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()