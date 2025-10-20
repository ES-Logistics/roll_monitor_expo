"""
Agendador de relatórios - Responsável pelo agendamento automático às 11h e 16h
"""

import schedule
import time
import threading
from datetime import datetime, timedelta
from .report_service import ReportService


class SchedulerService:
    """Service responsável pelo agendamento automático dos relatórios"""
    
    def __init__(self):
        self.report_service = ReportService()
        self.is_running = False
        self.scheduler_thread = None
        self.email_callback = None
    
    def set_email_callback(self, callback_function):
        """Define função de callback para envio automático de email"""
        self.email_callback = callback_function
    
    def _is_weekday(self):
        """Verifica se hoje é um dia útil (segunda a sexta)"""
        today = datetime.now().weekday()  # 0=segunda, 1=terça, ..., 6=domingo
        return today < 5  # 0-4 são dias úteis (segunda a sexta)
    
    def setup_schedule(self):
        """Configura os horários de execução automática apenas em dias úteis"""
        # Limpa agendamentos anteriores
        schedule.clear()
        
        # Agenda para 14:00 e 19:00 UTC (equivale a 11:00 e 16:00 BRT/UTC-3)
        schedule.every().monday.at("14:00").do(self._execute_scheduled_report, time="14:00 UTC (11:00 BRT)")
        schedule.every().monday.at("19:00").do(self._execute_scheduled_report, time="19:00 UTC (16:00 BRT)")
        schedule.every().tuesday.at("14:00").do(self._execute_scheduled_report, time="14:00 UTC (11:00 BRT)")
        schedule.every().tuesday.at("19:00").do(self._execute_scheduled_report, time="19:00 UTC (16:00 BRT)")
        schedule.every().wednesday.at("14:00").do(self._execute_scheduled_report, time="14:00 UTC (11:00 BRT)")
        schedule.every().wednesday.at("19:00").do(self._execute_scheduled_report, time="19:00 UTC (16:00 BRT)")
        schedule.every().thursday.at("14:00").do(self._execute_scheduled_report, time="14:00 UTC (11:00 BRT)")
        schedule.every().thursday.at("19:00").do(self._execute_scheduled_report, time="19:00 UTC (16:00 BRT)")
        schedule.every().friday.at("14:00").do(self._execute_scheduled_report, time="14:00 UTC (11:00 BRT)")
        schedule.every().friday.at("19:00").do(self._execute_scheduled_report, time="19:00 UTC (16:00 BRT)")
        
        print("Agendamento configurado para Servidor UTC:")
        print("  • 14:00 UTC - Relatório automático (11:00 horário de Brasília)")
        print("  • 19:00 UTC - Relatório automático (16:00 horário de Brasília)")
        print("  • APENAS DIAS ÚTEIS - Finais de semana: SEM EXECUÇÃO")
    
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
            # Validação extra: Verifica se é dia útil
            if not self._is_weekday():
                current_day = datetime.now().strftime('%A')  # Nome do dia da semana
                print(f"🚫 Execução pulada: {current_day} é final de semana")
                print("   Relatórios são enviados apenas em dias úteis (segunda a sexta)")
                return
            
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
                
                # Chama callback de email se configurado
                if self.email_callback and result.get('file_path'):
                    print(f"📧 Enviando relatório por email...")
                    try:
                        email_success = self.email_callback(result['file_path'])
                        if email_success:
                            print(f"✅ Relatório enviado por email com sucesso!")
                        else:
                            print(f"❌ Falha no envio por email")
                    except Exception as e:
                        print(f"❌ Erro no callback de email: {e}")
            else:
                if result.get('skipped'):
                    print(f"ℹ️  {result['message']}")
                else:
                    print(f"❌ {result['message']}")
            
        except Exception as e:
            print(f"❌ Erro na execução agendada: {e}")
    
    def get_next_executions(self):
        """Retorna informações sobre próximas execuções em dias úteis"""
        if not self.is_running:
            return "Agendador não está em execução"
        
        jobs = schedule.get_jobs()
        if not jobs:
            return "Nenhum agendamento ativo"
        
        next_runs = []
        weekdays = ['Segunda', 'Terça', 'Quarta', 'Quinta', 'Sexta', 'Sábado', 'Domingo']
        
        for job in jobs:
            next_run = job.next_run
            if next_run:
                day_name = weekdays[next_run.weekday()]
                # Só mostra se for dia útil
                if next_run.weekday() < 5:  # 0-4 são dias úteis
                    utc_time = next_run.strftime('%H:%M:%S')
                    # Converte UTC para horário de Brasília (UTC-3)
                    brt_hour = next_run.hour - 3
                    if brt_hour < 0:
                        brt_hour += 24
                    brt_time = f"{brt_hour:02d}:{next_run.minute:02d}:{next_run.second:02d}"
                    
                    next_runs.append(f"  • {next_run.strftime('%Y-%m-%d')} {utc_time} UTC ({brt_time} BRT) - {day_name}")
        
        if next_runs:
            result = "Próximas execuções (APENAS DIAS ÚTEIS):\n" + "\n".join(next_runs)
            result += "\n🌍 UTC = Horário do servidor | BRT = Horário de Brasília (UTC-3)"
            result += "\n📝 NOTA: Finais de semana são automaticamente pulados"
            return result
        else:
            return "Sem execuções agendadas para dias úteis"