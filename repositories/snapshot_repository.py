"""
Repository para gerenciar operações de snapshot no banco de dados
"""
import psycopg2
from services.datalake_connection import get_db_config

class SnapshotRepository:
    
    def __init__(self):
        self.table_name = "bronze.roll_monitor_snapshot"
    
    def save_snapshot(self, data_dict):
        """Salva o snapshot atual no banco, substituindo os dados anteriores"""
        conn = psycopg2.connect(**get_db_config())
        
        try:
            cursor = conn.cursor()
            
            # Truncate da tabela (limpa dados anteriores)
            cursor.execute(f"TRUNCATE TABLE {self.table_name}")
            
            # Insere novos dados
            insert_query = f"""
                INSERT INTO {self.table_name} 
                (unique_id, proceso, porto_embarque, navio_embarque, previsao_embarque, 
                 previsao_embarque_feeder, navio_feeder, porto_feeder, 
                 previsao_embarque_transbordo, porto_transbordo, navio_transbordo,
                 email_responsavel, armador, cliente)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            records_inserted = 0
            for unique_id, record in data_dict.items():
                    
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
                    record.get('navio_transbordo'),
                    record.get('email_responsavel'),
                    record.get('armador'),
                    record.get('cliente')
                ))
                records_inserted += 1
            
            conn.commit()
            return records_inserted
            
        except Exception as e:
            conn.rollback()
            raise Exception(f"Erro ao salvar snapshot: {e}")
        finally:
            cursor.close()
            conn.close()
    
    def load_snapshot(self, max_age_hours=1):
        """Carrega o snapshot atual do banco, considerando idade máxima dos dados"""
        conn = psycopg2.connect(**get_db_config())
        
        try:
            cursor = conn.cursor()
            
            # Verifica se existe snapshot recente
            cursor.execute(f"""
                SELECT COUNT(*), MAX(updated_at) 
                FROM {self.table_name}
            """)
            
            count_result = cursor.fetchone()
            record_count = count_result[0] if count_result else 0
            last_updated = count_result[1] if count_result else None
            
            # Se não há dados ou são muito antigos, retorna vazio
            if record_count == 0:
                return {}
            
            # Verifica se os dados são muito antigos usando o PostgreSQL (timezone-safe)
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM {self.table_name}
                WHERE updated_at > NOW() - INTERVAL '{max_age_hours} hours'
            """)
            
            recent_count = cursor.fetchone()[0] if cursor.fetchone else 0
            
            if recent_count == 0:
                from datetime import datetime, timedelta
                current_time = datetime.now()
                time_diff = current_time - last_updated.replace(tzinfo=None) if last_updated else "N/A"
                print(f"Snapshot muito antigo (diferença: {time_diff}). Reiniciando comparação.")
                print(f"Dados no banco: {record_count}, Dados recentes: {recent_count}, Max age: {max_age_hours}h")
                return {}
            
            # Carrega dados se estão atualizados
            cursor.execute(f"""
                SELECT unique_id, proceso, porto_embarque, navio_embarque, previsao_embarque, 
                       previsao_embarque_feeder, navio_feeder, porto_feeder, 
                       previsao_embarque_transbordo, porto_transbordo, navio_transbordo,
                       email_responsavel, armador, cliente
                FROM {self.table_name}
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
            
            return data_dict
            
        except Exception as e:
            raise Exception(f"Erro ao carregar snapshot: {e}")
        finally:
            cursor.close()
            conn.close()
    
    def get_snapshot_count(self):
        """Retorna o número de registros no snapshot atual"""
        conn = psycopg2.connect(**get_db_config())
        
        try:
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {self.table_name}")
            count = cursor.fetchone()[0]
            return count
            
        except Exception as e:
            raise Exception(f"Erro ao contar snapshot: {e}")
        finally:
            cursor.close()
            conn.close()