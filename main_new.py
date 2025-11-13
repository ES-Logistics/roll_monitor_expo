"""
Sistema de Monitoramento de Processos Roll Monitor Exportação
Aplicação principal que utiliza arquitetura modular com Controllers e Services
"""
from controllers.monitoring_controller import MonitoringController

def main():
    """Função principal da aplicação"""
    controller = MonitoringController()
    controller.start_monitoring(interval_minutes=3)

if __name__ == "__main__":
    main()