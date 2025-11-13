"""
Repository para gerenciar operações de mudanças no banco de dados
"""
import psycopg2
import json
from datetime import datetime
from services.datalake_connection import get_db_config

def serialize_datetime(obj):
    """Converte objetos datetime para string para serialização JSON"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    return obj

def prepare_for_json(data):
    """Prepara dados para serialização JSON convertendo datetime"""
    if isinstance(data, dict):
        return {k: prepare_for_json(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [prepare_for_json(item) for item in data]
    elif isinstance(data, datetime):
        return data.isoformat()
    else:
        return data

class ChangesRepository:
    
    def __init__(self):
        self.table_name = "bronze.roll_monitor_expo_changes"
    
    def register_changes(self, proceso, changes_detail):
        """Registra ou atualiza as alterações de um processo"""
        conn = psycopg2.connect(**get_db_config())
        
        try:
            cursor = conn.cursor()
            
            # Verifica se o processo já tem registro
            cursor.execute(
                f"SELECT alteracoes FROM {self.table_name} WHERE proceso = %s",
                (proceso,)
            )
            
            existing_record = cursor.fetchone()
            
            # Cria entrada de alteração com timestamp
            new_change = {
                'timestamp': datetime.now().isoformat(),
                'alteracoes': prepare_for_json(changes_detail)
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
                cursor.execute(f"""
                    UPDATE {self.table_name} 
                    SET alteracoes = %s, 
                        status_relatorio = 'PENDENTE',
                        updated_at = CURRENT_TIMESTAMP
                    WHERE proceso = %s
                """, (json.dumps(prepare_for_json(existing_alteracoes)), proceso))
                
            else:
                # Processo novo - cria primeiro registro
                new_alteracoes = {
                    'processo': proceso,
                    'primeira_deteccao': datetime.now().isoformat(),
                    'ultima_alteracao': datetime.now().isoformat(),
                    'historico_alteracoes': [new_change]
                }
                
                cursor.execute(f"""
                    INSERT INTO {self.table_name} (proceso, alteracoes, status_relatorio)
                    VALUES (%s, %s, 'PENDENTE')
                """, (proceso, json.dumps(prepare_for_json(new_alteracoes))))
            
            conn.commit()
            return True
            
        except Exception as e:
            conn.rollback()
            raise Exception(f"Erro ao registrar alterações do processo {proceso}: {e}")
        finally:
            cursor.close()
            conn.close()
    
    def get_pending_changes(self):
        """Retorna todos os processos com alterações pendentes de relatório"""
        conn = psycopg2.connect(**get_db_config())
        
        try:
            cursor = conn.cursor()
            
            cursor.execute(f"""
                SELECT proceso, alteracoes, created_at, updated_at
                FROM {self.table_name} 
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
            raise Exception(f"Erro ao buscar alterações pendentes: {e}")
        finally:
            cursor.close()
            conn.close()
    
    def mark_as_reported(self, processos_list):
        """Marca uma lista de processos como já incluídos no relatório"""
        if not processos_list:
            return 0
            
        conn = psycopg2.connect(**get_db_config())
        
        try:
            cursor = conn.cursor()
            
            # Atualiza status para ENVIADO
            cursor.execute(f"""
                UPDATE {self.table_name} 
                SET status_relatorio = 'ENVIADO',
                    updated_at = CURRENT_TIMESTAMP
                WHERE proceso = ANY(%s)
            """, (processos_list,))
            
            updated_count = cursor.rowcount
            conn.commit()
            return updated_count
            
        except Exception as e:
            conn.rollback()
            raise Exception(f"Erro ao marcar como enviado: {e}")
        finally:
            cursor.close()
            conn.close()
    
    def get_changes_by_proceso(self, proceso):
        """Retorna as alterações de um processo específico"""
        conn = psycopg2.connect(**get_db_config())
        
        try:
            cursor = conn.cursor()
            
            cursor.execute(f"""
                SELECT proceso, alteracoes, status_relatorio, created_at, updated_at
                FROM {self.table_name} 
                WHERE proceso = %s
            """, (proceso,))
            
            row = cursor.fetchone()
            
            if row:
                return {
                    'proceso': row[0],
                    'alteracoes': row[1],
                    'status_relatorio': row[2],
                    'created_at': row[3],
                    'updated_at': row[4]
                }
            
            return None
            
        except Exception as e:
            raise Exception(f"Erro ao buscar alterações do processo {proceso}: {e}")
        finally:
            cursor.close()
            conn.close()