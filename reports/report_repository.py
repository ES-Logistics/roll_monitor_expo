"""
Repository para operações relacionadas a relatórios na tabela de changes
"""

import psycopg2
import json
from datetime import datetime
from services.datalake_connection import get_db_config


class ReportRepository:
    """Repository para consultas e operações de relatório"""
    
    def __init__(self):
        self.changes_table = "bronze.roll_monitor_expo_changes"
        self.snapshot_table = "bronze.roll_monitor_expo_snapshot"
    
    def get_pending_changes(self):
        """Busca todos os processos com status PENDENTE para relatório"""
        conn = psycopg2.connect(**get_db_config())
        
        try:
            cursor = conn.cursor()
            
            # Busca processos pendentes com seus dados de mudança
            cursor.execute(f"""
                SELECT c.proceso, c.alteracoes, c.created_at, c.updated_at
                FROM {self.changes_table} c
                WHERE c.status_relatorio = 'PENDENTE'
                ORDER BY c.updated_at DESC
            """)
            
            pending_records = cursor.fetchall()
            
            if not pending_records:
                return []
            
            # Para cada processo pendente, busca o estado atual do snapshot
            result = []
            for record in pending_records:
                proceso = record[0]
                alteracoes = record[1]
                created_at = record[2]
                updated_at = record[3]
                
                # Busca estado atual do processo
                current_state = self._get_current_process_state(cursor, proceso)
                
                result.append({
                    'proceso': proceso,
                    'alteracoes': alteracoes,
                    'current_state': current_state,
                    'created_at': created_at,
                    'updated_at': updated_at
                })
            
            return result
            
        except Exception as e:
            print(f"Erro ao buscar mudanças pendentes: {e}")
            return []
        finally:
            cursor.close()
            conn.close()
    
    def _get_current_process_state(self, cursor, proceso):
        """Busca o estado atual de um processo no snapshot"""
        cursor.execute(f"""
            SELECT proceso, porto_embarque, navio_embarque, previsao_embarque,
                   previsao_embarque_transbordo, porto_transbordo, navio_transbordo,
                   porto_destino, email_responsavel, armador, cliente, motivo_transferencia
            FROM {self.snapshot_table}
            WHERE proceso = %s
        """, (proceso,))
        
        row = cursor.fetchone()
        if not row:
            return None
        
        columns = ['proceso', 'porto_embarque', 'navio_embarque', 'previsao_embarque',
                  'previsao_embarque_transbordo', 'porto_transbordo', 'navio_transbordo',
                  'porto_destino', 'email_responsavel', 'armador', 'cliente', 'motivo_transferencia']
        
        return dict(zip(columns, row))
    
    def mark_processes_as_sent(self, processos_list):
        """Marca uma lista de processos como ENVIADO no relatório"""
        if not processos_list:
            return True
        
        conn = psycopg2.connect(**get_db_config())
        
        try:
            cursor = conn.cursor()
            
            # Converte lista para formato adequado para SQL IN
            placeholders = ','.join(['%s'] * len(processos_list))
            
            cursor.execute(f"""
                UPDATE {self.changes_table} 
                SET status_relatorio = 'ENVIADO',
                    updated_at = CURRENT_TIMESTAMP
                WHERE proceso IN ({placeholders})
                AND status_relatorio = 'PENDENTE'
            """, processos_list)
            
            affected_rows = cursor.rowcount
            conn.commit()
            
            print(f"Marcados como enviados: {affected_rows} processos")
            return True
            
        except Exception as e:
            print(f"Erro ao marcar processos como enviados: {e}")
            conn.rollback()
            return False
        finally:
            cursor.close()
            conn.close()
    
    def get_report_statistics(self):
        """Retorna estatísticas para relatório"""
        conn = psycopg2.connect(**get_db_config())
        
        try:
            cursor = conn.cursor()
            
            cursor.execute(f"""
                SELECT 
                    COUNT(*) as total_processos,
                    COUNT(CASE WHEN status_relatorio = 'PENDENTE' THEN 1 END) as pendentes,
                    COUNT(CASE WHEN status_relatorio = 'ENVIADO' THEN 1 END) as enviados
                FROM {self.changes_table}
            """)
            
            result = cursor.fetchone()
            
            return {
                'total_processos': result[0],
                'pendentes': result[1], 
                'enviados': result[2]
            }
            
        except Exception as e:
            print(f"Erro ao buscar estatísticas: {e}")
            return {'total_processos': 0, 'pendentes': 0, 'enviados': 0}
        finally:
            cursor.close()
            conn.close()