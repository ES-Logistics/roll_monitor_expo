"""
Controller de relatórios Excel - Orquestra a geração de relatórios e agendamento
"""

import os
from datetime import datetime
from .report_service import ReportService
from .scheduler_service import SchedulerService


class ExcelReportController:
    """Controller principal para operações de relatório Excel"""
    
    def __init__(self):
        self.report_service = ReportService()
        self.scheduler_service = SchedulerService()
        self.is_monitoring = False
    
    def generate_manual_report(self):
        """Gera relatório manualmente (comando direto do usuário)"""
        print(f"\n=== Geração Manual de Relatório - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===")
        
        # Mostra resumo antes de gerar
        summary = self.report_service.get_pending_summary()
        
        if summary['success']:
            print(f"📊 Status: {summary['message']}")
            if summary.get('statistics'):
                stats = summary['statistics']
                print(f"   Total de processos: {stats['total_processos']}")
                print(f"   Pendentes: {stats['pendentes']}")
                print(f"   Já enviados: {stats['enviados']}")
            
            if not summary['has_pending']:
                print("ℹ️  Nenhum relatório será gerado.")
                return summary
        
        # Confirma geração
        if summary.get('has_pending'):
            print(f"\n🎯 Processos que serão incluídos:")
            for processo in summary.get('pending_processes', []):
                print(f"   • {processo}")
        
        # Gera relatório
        result = self.report_service.generate_excel_report(manual=True)
        
        if result['success']:
            print(f"\n✅ {result['message']}")
            print(f"📁 Arquivo salvo: {result['filename']}")
            print(f"📂 Diretório: {os.path.dirname(result['file_path'])}")
            print(f"📊 Processos incluídos: {result['processes_count']}")
        else:
            print(f"\n❌ {result['message']}")
            if 'error' in result:
                print(f"Detalhes: {result['error']}")
        
        return result
    
    def start_automatic_reports(self):
        """Inicia sistema de relatórios automáticos (11h e 16h)"""
        print(f"\n=== Iniciando Sistema de Relatórios Automáticos ===")
        
        try:
            self.scheduler_service.start_scheduler()
            self.is_monitoring = True
            
            print("✅ Sistema de relatórios automáticos iniciado")
            print(self.scheduler_service.get_next_executions())
            
            return {
                'success': True,
                'message': 'Sistema de relatórios automáticos iniciado'
            }
            
        except Exception as e:
            print(f"❌ Erro ao iniciar sistema automático: {e}")
            return {
                'success': False,
                'message': f'Erro ao iniciar: {e}'
            }
    
    def stop_automatic_reports(self):
        """Para sistema de relatórios automáticos"""
        print("\n=== Parando Sistema de Relatórios Automáticos ===")
        
        try:
            self.scheduler_service.stop_scheduler()
            self.is_monitoring = False
            
            print("✅ Sistema de relatórios automáticos parado")
            
            return {
                'success': True,
                'message': 'Sistema parado com sucesso'
            }
            
        except Exception as e:
            print(f"❌ Erro ao parar sistema: {e}")
            return {
                'success': False,
                'message': f'Erro ao parar: {e}'
            }
    
    def get_report_status(self):
        """Retorna status geral do sistema de relatórios"""
        print(f"\n=== Status do Sistema de Relatórios ===")
        
        # Status do agendador
        scheduler_status = "🟢 Ativo" if self.is_monitoring else "🔴 Inativo"
        print(f"Agendador automático: {scheduler_status}")
        
        if self.is_monitoring:
            print(self.scheduler_service.get_next_executions())
        
        # Estatísticas de pendências
        summary = self.report_service.get_pending_summary()
        if summary['success']:
            print(f"\n📊 {summary['message']}")
            if summary.get('statistics'):
                stats = summary['statistics']
                print(f"   Total de processos: {stats['total_processos']}")
                print(f"   Pendentes: {stats['pendentes']}")
                print(f"   Já enviados: {stats['enviados']}")
        
        return {
            'scheduler_active': self.is_monitoring,
            'summary': summary
        }
    
    def list_generated_reports(self):
        """Lista relatórios gerados recentemente"""
        reports_dir = self.report_service.reports_dir
        
        if not os.path.exists(reports_dir):
            print("📂 Nenhum diretório de relatórios encontrado")
            return []
        
        # Lista arquivos Excel no diretório
        excel_files = []
        for filename in os.listdir(reports_dir):
            if filename.endswith('.xlsx') and filename.startswith('Relatorio_Changes_'):
                file_path = os.path.join(reports_dir, filename)
                file_stats = os.stat(file_path)
                file_size = file_stats.st_size / 1024  # KB
                modified_time = datetime.fromtimestamp(file_stats.st_mtime)
                
                excel_files.append({
                    'filename': filename,
                    'size_kb': round(file_size, 2),
                    'modified': modified_time,
                    'path': file_path
                })
        
        # Ordena por data de modificação (mais recente primeiro)
        excel_files.sort(key=lambda x: x['modified'], reverse=True)
        
        print(f"\n📁 Relatórios Gerados ({len(excel_files)} arquivos):")
        if excel_files:
            for file_info in excel_files[:10]:  # Mostra últimos 10
                print(f"   • {file_info['filename']}")
                print(f"     Tamanho: {file_info['size_kb']} KB | Modificado: {file_info['modified'].strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            print("   Nenhum relatório encontrado")
        
        return excel_files