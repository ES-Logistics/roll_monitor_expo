"""
Sistema de Monitoramento de Processos Roll Monitor Exportacao
Aplicacao principal com subcomandos: monitor e send-report
"""
import argparse
import logging
import dotenv

from services.mailing_serivce import MailReport
from services.query_service import QueryService
from controllers.loop_controller import Monitor


def cmd_monitor(args):
    monitor = Monitor()
    monitor.run()


def cmd_send_report(args):
    report = MailReport()
    report.send()
    QueryService().mark_report_as_sent()


def main():
    dotenv.load_dotenv()
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="Roll Monitor Expo")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("monitor", help="Executa um ciclo de monitoramento (snapshot + diff)")
    subparsers.add_parser("send-report", help="Envia o relatorio de alterações por email")

    args = parser.parse_args()

    commands = {
        "monitor": cmd_monitor,
        "send-report": cmd_send_report,
    }
    commands[args.command](args)


if __name__ == "__main__":
    main()
