"""
Service de relatórios - Lógica de negócio para geração de relatórios Excel
"""

import os
from datetime import datetime
from .report_repository import ReportRepository
from .excel_formatter import ExcelFormatter


class ReportService:
    """Service responsável pela lógica de negócio dos relatórios"""
    
    def __init__(self):
        self.repository = ReportRepository()
        self.reports_dir = self._ensure_reports_directory()
    
    def generate_excel_report(self, manual=False):
        """
        Gera relatório Excel com processos pendentes
        
        Args:
            manual (bool): Se True, é execução manual; se False, é automática
            
        Returns:
            dict: Resultado da operação com success, file_path, message, etc.
        """
        try:
            # 1. Verifica se há mudanças pendentes
            statistics = self.repository.get_report_statistics()
            pending_count = statistics['pendentes']
            
            if pending_count == 0:
                return {
                    'success': False,
                    'message': 'Nenhuma mudança pendente para relatório',
                    'statistics': statistics,
                    'skipped': True
                }
            
            # 2. Busca dados pendentes
            pending_data = self.repository.get_pending_changes()
            
            if not pending_data:
                return {
                    'success': False,
                    'message': 'Erro ao buscar dados pendentes',
                    'statistics': statistics
                }
            
            # 3. Gera arquivo Excel
            formatter = ExcelFormatter()
            workbook = formatter.create_report(pending_data)
            
            # 4. Define nome e caminho do arquivo
            timestamp = datetime.now().strftime('%Y-%m-%d_%Hh%M')
            execution_type = 'Manual' if manual else 'Auto'
            filename = f"Relatorio_Changes_{execution_type}_{timestamp}.xlsx"
            file_path = os.path.join(self.reports_dir, filename)
            
            # 5. Salva arquivo
            workbook.save(file_path)
            
            # 6. Marca processos como enviados
            processos_enviados = [item['proceso'] for item in pending_data]
            mark_success = self.repository.mark_processes_as_sent(processos_enviados)
            
            if not mark_success:
                return {
                    'success': False,
                    'message': 'Relatório gerado mas erro ao marcar como enviado',
                    'file_path': file_path,
                    'processes_count': len(processos_enviados)
                }
            
            return {
                'success': True,
                'message': f'Relatório gerado com sucesso: {len(processos_enviados)} processos',
                'file_path': file_path,
                'filename': filename,
                'processes_count': len(processos_enviados),
                'statistics': statistics,
                'execution_type': execution_type
            }
            
        except Exception as e:
            return {
                'success': False,
                'message': f'Erro ao gerar relatório: {str(e)}',
                'error': str(e)
            }
    
    def get_pending_summary(self):
        """Retorna resumo dos processos pendentes sem gerar relatório"""
        try:
            statistics = self.repository.get_report_statistics()
            
            if statistics['pendentes'] == 0:
                return {
                    'success': True,
                    'message': 'Nenhum processo pendente',
                    'statistics': statistics,
                    'has_pending': False
                }
            
            # Busca lista de processos pendentes (só os nomes)
            pending_data = self.repository.get_pending_changes()
            pending_processes = [item['proceso'] for item in pending_data]
            
            return {
                'success': True,
                'message': f'{statistics["pendentes"]} processos pendentes para relatório',
                'statistics': statistics,
                'has_pending': True,
                'pending_processes': pending_processes
            }
            
        except Exception as e:
            return {
                'success': False,
                'message': f'Erro ao buscar resumo: {str(e)}',
                'error': str(e)
            }
    
    def _ensure_reports_directory(self):
        """Garante que o diretório de relatórios existe"""
        current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        reports_dir = os.path.join(current_dir, 'generated_reports')
        
        if not os.path.exists(reports_dir):
            os.makedirs(reports_dir)
            print(f"Diretório de relatórios criado: {reports_dir}")
        
        return reports_dir