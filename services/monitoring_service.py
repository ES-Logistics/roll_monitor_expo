"""
Service para gerenciar operações de monitoramento de processos
"""
from repositories.snapshot_repository import SnapshotRepository
from repositories.changes_repository import ChangesRepository

class MonitoringService:
    
    def __init__(self):
        self.snapshot_repo = SnapshotRepository()
        self.changes_repo = ChangesRepository()
    
    def save_current_snapshot(self, data_dict):
        """Salva o snapshot atual dos dados"""
        try:
            records_count = self.snapshot_repo.save_snapshot(data_dict)
            return {
                'success': True,
                'records_saved': records_count,
                'message': f"Snapshot salvo: {records_count} registros"
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'message': f"Erro ao salvar snapshot: {e}"
            }
    
    def load_previous_snapshot(self, max_age_hours=1):
        """Carrega o snapshot anterior dos dados, considerando idade máxima"""
        try:
            data_dict = self.snapshot_repo.load_snapshot(max_age_hours)
            return {
                'success': True,
                'data': data_dict,
                'records_count': len(data_dict),
                'message': f"Snapshot carregado: {len(data_dict)} registros"
            }
        except Exception as e:
            return {
                'success': False,
                'data': {},
                'error': str(e),
                'message': f"Erro ao carregar snapshot: {e}"
            }
    
    def register_process_changes(self, proceso, changes_detail):
        """Registra alterações de um processo"""
        try:
            success = self.changes_repo.register_changes(proceso, changes_detail)
            return {
                'success': success,
                'message': f"Alterações registradas para processo: {proceso}"
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'message': f"Erro ao registrar alterações: {e}"
            }
    
    def get_pending_reports(self):
        """Retorna processos com alterações pendentes para relatório"""
        try:
            pending_changes = self.changes_repo.get_pending_changes()
            return {
                'success': True,
                'data': pending_changes,
                'count': len(pending_changes),
                'message': f"Encontrados {len(pending_changes)} processos com alterações pendentes"
            }
        except Exception as e:
            return {
                'success': False,
                'data': [],
                'error': str(e),
                'message': f"Erro ao buscar alterações pendentes: {e}"
            }
    
    def mark_reports_as_sent(self, processos_list):
        """Marca processos como já incluídos em relatório"""
        try:
            updated_count = self.changes_repo.mark_as_reported(processos_list)
            return {
                'success': True,
                'updated_count': updated_count,
                'message': f"Marcados como enviados: {updated_count} processos"
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'message': f"Erro ao marcar como enviado: {e}"
            }
    
    def get_process_changes(self, proceso):
        """Retorna o histórico de alterações de um processo específico"""
        try:
            changes = self.changes_repo.get_changes_by_proceso(proceso)
            return {
                'success': True,
                'data': changes,
                'message': f"Alterações do processo {proceso} obtidas com sucesso"
            }
        except Exception as e:
            return {
                'success': False,
                'data': None,
                'error': str(e),
                'message': f"Erro ao buscar alterações do processo: {e}"
            }