"""
Script de demonstração e uso do sistema de relatórios Excel
Como usar o sistema de relatórios em produção
"""

from reports.excel_report_controller import ExcelReportController
from controllers.mail_controller import MailController
import sys
import os
import glob
import shutil

def clean_generated_reports():
    """Limpa a pasta de relatórios gerados"""
    try:
        reports_folder = "generated_reports"
        if os.path.exists(reports_folder):
            # Remove todos os arquivos da pasta
            files = glob.glob(os.path.join(reports_folder, "*"))
            for file in files:
                if os.path.isfile(file):
                    os.remove(file)
                    print(f"🗑️ Arquivo removido: {os.path.basename(file)}")
            print(f"✅ Pasta {reports_folder} limpa com sucesso!")
        else:
            print(f"⚠️ Pasta {reports_folder} não existe")
            
    except Exception as e:
        print(f"❌ Erro ao limpar pasta de relatórios: {e}")

def send_report_by_email(report_path, target_emails, output_email):
    """Envia relatório por email usando o mail controller"""
    try:
        mail_controller = MailController()
        
        # Configurar dados do email
        subject = f"Relatório Roll Monitor Exportação - {os.path.basename(report_path)}"
        body = f"""
        <html>
        <body>
            <h2>📊 Relatório Roll Monitor Exportação</h2>
            <p>Segue em anexo o relatório de monitoramento de processos.</p>
            <p><strong>Arquivo:</strong> {os.path.basename(report_path)}</p>
            <p><strong>Data de Geração:</strong> {os.path.getctime(report_path)}</p>
            <br>
            <p><em>Este é um email automático do sistema Roll Monitor Exportação.</em></p>
        </body>
        </html>
        """
        
        # Se target_emails é uma lista, envia para cada um
        if isinstance(target_emails, list):
            success_count = 0
            for email in target_emails:
                print(f"📧 Enviando para: {email}")
                success = mail_controller.send_email_with_attachment(
                    to_email=email,
                    from_email=output_email,
                    subject=subject,
                    body=body,
                    attachment_path=report_path,
                    attachment_name=os.path.basename(report_path)
                )
                if success:
                    success_count += 1
                    print(f"✅ Enviado com sucesso para {email}")
                else:
                    print(f"❌ Falha no envio para {email}")
            
            # Retorna True se pelo menos um envio foi bem-sucedido
            return success_count > 0
        else:
            # Se é uma string, envia para um único destinatário
            success = mail_controller.send_email_with_attachment(
                to_email=target_emails,
                from_email=output_email,
                subject=subject,
                body=body,
                attachment_path=report_path,
                attachment_name=os.path.basename(report_path)
            )
            return success
        
    except Exception as e:
        print(f"❌ Erro ao enviar email: {e}")
        return False

def show_menu():
    """Mostra menu de opções disponíveis"""
    print("\n" + "=" * 60)
    print("📊 SISTEMA DE RELATÓRIOS EXCEL - ROLL MONITOR EXPORTAÇÃO")
    print("   🎯 MONITORA APENAS: navio_embarque e navio_transbordo")
    print("=" * 60)
    print("1. 📋 Verificar Status do Sistema")
    print("2. 📁 Listar Relatórios Gerados")
    print("3. 🎯 Gerar Relatório Manual")
    print("4. ⏰ Iniciar Relatórios Automáticos com Email (11h e 16h)")
    print("5. 📧 Enviar Relatórios Existentes por Email")
    print("6. 🗑️ Limpar Pasta de Relatórios")
    print("7. 🛑 Parar Relatórios Automáticos")
    print("8. ❌ Sair")
    print("=" * 60)
    print("💡 IMPORTANTE: Apenas mudanças em navios geram relatórios")
    print("=" * 60)

def main():
    """Função principal do sistema de relatórios"""
    
    # ====== CONFIGURAÇÕES DE EMAIL ======
    target_mail = [
        "guilherme.decker@eslogistics.com.br",
        "rodrigo@eslogistics.com.br",
        "bianca.santos@esglobal.com.br",
        "paola.sampaio@esglobal.com.br",
        "rafael.eiccholz@esglobal.com.br",
        "eduardo.pereira@esglobal.com.br",
        "roger.santos@esglobal.com.br"
    ]  # Lista de emails de destino
    output_mail = "inputs_datalake@eslogistics.com.br"  # Email de origem
    # ====================================
    
    controller = ExcelReportController()
    
    print("🚀 Iniciando Sistema de Relatórios Excel...")
    print("   Desenvolvido para monitoramento de processos Roll Monitor Exportação")
    print("   🎯 Sistema monitora apenas: navio_embarque e navio_transbordo")
    print("   🚫 Outros campos são ignorados para geração de relatórios")
    
    # Iniciar automaticamente na opção 4 (Relatórios Automáticos com Email)
    auto_start = True
    
    while True:
        if auto_start:
            choice = '4'
            auto_start = False  # Só executa automaticamente na primeira vez
        else:
            show_menu()
            choice = input("\nEscolha uma opção (1-8): ").strip()
        
        try:
            if choice == '1':
                controller.get_report_status()
                
            elif choice == '2':
                controller.list_generated_reports()
                
            elif choice == '3':
                print("\n🎯 Iniciando geração manual...")
                result = controller.generate_manual_report()
                
                if result.get('success'):
                    # Pergunta se quer abrir o arquivo
                    open_file = input(f"\n📂 Deseja abrir o arquivo gerado? (s/n): ").strip().lower()
                    if open_file in ['s', 'sim', 'y', 'yes']:
                        os.startfile(result['file_path'])
                
            elif choice == '4':
                print("\n⏰ Configurando relatórios automáticos com envio por email...")
                
                # Configurar callback de email
                def email_callback(report_path):
                    """Callback para envio automático de email após geração de relatório"""
                    success = send_report_by_email(report_path, target_mail, output_mail)
                    if success:
                        # Limpar relatório após envio
                        try:
                            os.remove(report_path)
                            print(f"🗑️ Relatório removido após envio: {os.path.basename(report_path)}")
                        except Exception as e:
                            print(f"⚠️ Erro ao remover relatório: {e}")
                    return success
                
                # Configurar callback no controller
                controller.set_email_callback(email_callback)
                
                result = controller.start_automatic_reports()
                
                if result['success']:
                    print("\n✅ Sistema automático em execução!")
                    print("📧 Relatórios serão enviados automaticamente por email após geração")
                    print("🗑️ Relatórios serão limpos automaticamente após envio")
                    print("💡 DICA: Mantenha este script rodando para que os relatórios")
                    print("   sejam gerados automaticamente às 11h e 16h.")
                    print("   Pressione Ctrl+C para parar quando necessário.")
                    
                    try:
                        print("\n⌛ Aguardando execuções agendadas...")
                        print("   (Pressione Ctrl+C para voltar ao menu)")
                        
                        # Mantém o agendador rodando
                        while True:
                            import time
                            time.sleep(60)  # Verifica a cada minuto
                            
                    except KeyboardInterrupt:
                        print("\n\n🔄 Voltando ao menu principal...")
                        print("   (O agendador continua ativo)")
                
            elif choice == '5':
                print("\n📧 Enviando relatórios existentes por email...")
                reports_folder = "generated_reports"
                if os.path.exists(reports_folder):
                    reports = glob.glob(os.path.join(reports_folder, "*.xlsx"))
                    if reports:
                        for report in reports:
                            print(f"\n📧 Enviando: {os.path.basename(report)}")
                            if send_report_by_email(report, target_mail, output_mail):
                                print(f"✅ Enviado com sucesso!")
                            else:
                                print(f"❌ Falha no envio")
                        
                        # Perguntar se quer limpar após envio
                        limpar = input("\n🗑️ Deseja limpar a pasta após envio? (s/n): ").strip().lower()
                        if limpar in ['s', 'sim', 'y', 'yes']:
                            clean_generated_reports()
                    else:
                        print("⚠️ Nenhum relatório encontrado para envio")
                else:
                    print("⚠️ Pasta de relatórios não existe")
                
            elif choice == '6':
                print("\n🗑️ Limpando pasta de relatórios...")
                clean_generated_reports()
                
            elif choice == '7':
                print("\n🛑 Parando sistema automático...")
                result = controller.stop_automatic_reports()
                
            elif choice == '8':
                print("\n👋 Encerrando sistema...")
                
                # Para agendador se estiver ativo
                if controller.is_monitoring:
                    print("🛑 Parando agendador automático...")
                    controller.stop_automatic_reports()
                
                print("✅ Sistema encerrado. Até logo!")
                sys.exit(0)
                
            else:
                print("❌ Opção inválida! Escolha entre 1-8.")
                
        except KeyboardInterrupt:
            print("\n\n🔄 Voltando ao menu...")
            continue
        except Exception as e:
            print(f"\n❌ Erro inesperado: {e}")
            continue
        
        # Pausa para ver resultado
        input("\n⏸️  Pressione Enter para continuar...")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n👋 Sistema interrompido pelo usuário. Até logo!")
    except Exception as e:
        print(f"\n❌ Erro crítico: {e}")
        sys.exit(1)