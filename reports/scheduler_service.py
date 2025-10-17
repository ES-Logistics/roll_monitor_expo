"""
Agendador de relatórios - Responsável pelo agendamento automático às 11h e 16h
"""

import schedule
import time
import threading
from datetime import datetime
from .report_service import ReportService


class SchedulerService:
    """Service responsável pelo agendamento automático dos relatórios"""
    
    def __init__(self):
        self.report_service = ReportService()
        self.is_running = False
        self.scheduler_thread = None
    
    def setup_schedule(self):
        """Configura os horários de execução automática"""
        # Limpa agendamentos anteriores
        schedule.clear()
        
        # Agenda para 11:00 e 16:00
        schedule.every().day.at("11:00").do(self._execute_scheduled_report, time="11:00")
        schedule.every().day.at("16:00").do(self._execute_scheduled_report, time="16:00")
        
        print("Agendamento configurado:")
        print("  • 11:00 - Relatório automático")
        print("  • 16:00 - Relatório automático")
    
    def start_scheduler(self):
        """Inicia o agendador em thread separada"""
        if self.is_running:
            print("Agendador já está em execução")
            return
        
        self.setup_schedule()
        self.is_running = True
        
        # Executa em thread separada para não bloquear
        self.scheduler_thread = threading.Thread(target=self._run_scheduler, daemon=True)
        self.scheduler_thread.start()
        
        print(f"Agendador iniciado às {datetime.now().strftime('%H:%M:%S')}")
    
    def stop_scheduler(self):
        """Para o agendador"""
        self.is_running = False
        schedule.clear()
        print("Agendador parado")
    
    def _run_scheduler(self):
        """Loop principal do agendador"""
        while self.is_running:
            try:
                schedule.run_pending()
                time.sleep(30)  # Verifica a cada 30 segundos
            except Exception as e:
                print(f"Erro no agendador: {e}")
                time.sleep(60)  # Aguarda 1 minuto em caso de erro
    
    def _execute_scheduled_report(self, time):
        """Executa relatório agendado com validações"""
        print(f"\n=== Execução Agendada {time} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===")
        
        try:
            # Verifica se há mudanças pendentes antes de gerar
            summary = self.report_service.get_pending_summary()
            
            if not summary['success']:
                print(f"❌ Erro ao verificar pendências: {summary['message']}")
                return
            
            if not summary['has_pending']:
                print(f"ℹ️  {summary['message']} - Pulando execução")
                return
            
            # Gera relatório automático
            result = self.report_service.generate_excel_report(manual=False)
            
            if result['success']:
                print(f"✅ {result['message']}")
                print(f"📁 Arquivo: {result['filename']}")
                print(f"📊 Processos incluídos: {result['processes_count']}")
            else:
                if result.get('skipped'):
                    print(f"ℹ️  {result['message']}")
                else:
                    print(f"❌ {result['message']}")
            
        except Exception as e:
            print(f"❌ Erro na execução agendada: {e}")
    
    def get_next_executions(self):
        """Retorna informações sobre próximas execuções"""
        if not self.is_running:
            return "Agendador não está em execução"
        
        jobs = schedule.get_jobs()
        if not jobs:
            return "Nenhum agendamento ativo"
        
        next_runs = []
        for job in jobs:
            next_run = job.next_run
            if next_run:
                next_runs.append(f"  • {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
        
        return "Próximas execuções:\n" + "\n".join(next_runs) if next_runs else "Sem execuções agendadas"