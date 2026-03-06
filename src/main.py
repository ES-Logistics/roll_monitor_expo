"""
Sistema de Monitoramento de Processos Roll Monitor Exportação
Aplicação principal que utiliza arquitetura modular com Controllers e Services
"""
import logging
import dotenv
from controllers.loop_controller import Loop
from services.mailing_serivce import MailReport

def main():
    dotenv.load_dotenv()
    logging.basicConfig(level=logging.INFO)
    logging.getLogger(__name__)

    """Função principal da aplicação"""
    controller = Loop()
    controller.start_loop()


if __name__ == "__main__":
    main()
    