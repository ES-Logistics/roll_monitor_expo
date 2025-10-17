"""
Script de demonstração e uso do sistema de relatórios Excel
Como usar o sistema de relatórios em produção
"""

from reports.excel_report_controller import ExcelReportController
import sys

def show_menu():
    """Mostra menu de opções disponíveis"""
    print("\n" + "=" * 60)
    print("📊 SISTEMA DE RELATÓRIOS EXCEL - ROLL MONITOR")
    print("=" * 60)
    print("1. 📋 Verificar Status do Sistema")
    print("2. 📁 Listar Relatórios Gerados")
    print("3. 🎯 Gerar Relatório Manual")
    print("4. ⏰ Iniciar Relatórios Automáticos (11h e 16h)")
    print("5. 🛑 Parar Relatórios Automáticos")
    print("6. ❌ Sair")
    print("=" * 60)

def main():
    """Função principal do sistema de relatórios"""
    
    controller = ExcelReportController()
    
    print("🚀 Iniciando Sistema de Relatórios Excel...")
    print("   Desenvolvido para monitoramento de processos Roll Monitor")
    
    while True:
        show_menu()
        
        try:
            choice = input("\nEscolha uma opção (1-6): ").strip()
            
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
                        import os
                        os.startfile(result['file_path'])
                
            elif choice == '4':
                print("\n⏰ Configurando relatórios automáticos...")
                result = controller.start_automatic_reports()
                
                if result['success']:
                    print("\n✅ Sistema automático em execução!")
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
                print("\n🛑 Parando sistema automático...")
                result = controller.stop_automatic_reports()
                
            elif choice == '6':
                print("\n👋 Encerrando sistema...")
                
                # Para agendador se estiver ativo
                if controller.is_monitoring:
                    print("🛑 Parando agendador automático...")
                    controller.stop_automatic_reports()
                
                print("✅ Sistema encerrado. Até logo!")
                sys.exit(0)
                
            else:
                print("❌ Opção inválida! Escolha entre 1-6.")
                
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