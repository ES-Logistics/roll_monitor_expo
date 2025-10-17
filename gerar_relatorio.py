"""
Script simples para gerar relatório Excel manualmente
"""

from reports.excel_report_controller import ExcelReportController
import os

def gerar_relatorio():
    """Gera relatório Excel de forma simples e direta"""
    
    print("📊 GERADOR DE RELATÓRIO EXCEL - VERSÃO COM ORIGINAL")
    print("=" * 50)
    
    controller = ExcelReportController()
    
    # Verifica pendências
    print("🔍 Verificando mudanças pendentes...")
    summary = controller.report_service.get_pending_summary()
    
    if not summary.get('has_pending'):
        print(f"ℹ️  {summary['message']}")
        print("   Não há relatório para gerar no momento.")
        return
    
    print(f"✅ {summary['message']}")
    
    # Gera relatório
    print(f"\n🎯 Gerando relatório Excel com versão 0 'Original'...")
    result = controller.generate_manual_report()
    
    if result.get('success'):
        print(f"\n✅ SUCESSO!")
        print(f"📁 Arquivo: {result['filename']}")
        print(f"📊 Processos: {result['processes_count']}")
        print(f"\n🎨 NOVA ESTRUTURA:")
        print(f"   🔵 Linha AZUL: Estado atual (versão mais alta)")
        print(f"   🟤 Linha(s) BEGE: Histórico de mudanças")
        print(f"   🟤 Linha BEGE 'Original': Versão .0 - estado inicial")
        
        # Abre arquivo
        print(f"\n📂 Abrindo arquivo...")
        os.startfile(result['file_path'])
        
    else:
        print(f"\n❌ ERRO: {result.get('message')}")

if __name__ == "__main__":
    try:
        gerar_relatorio()
    except Exception as e:
        print(f"\n❌ Erro: {e}")
    
    input("\n⏸️  Pressione Enter para fechar...")