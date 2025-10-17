"""
Debug da formatação Excel - verifica se negrito+itálico+sublinhado está sendo aplicado
"""

from reports.report_repository import ReportRepository
from reports.excel_formatter import ExcelFormatter

def debug_formatting():
    """Debug detalhado da formatação aplicada"""
    
    print("🔍 DEBUG DA FORMATAÇÃO EXCEL")
    print("=" * 50)
    
    # 1. Busca dados pendentes
    repo = ReportRepository()
    pending_data = repo.get_pending_changes()
    
    if not pending_data:
        print("❌ Nenhum dado pendente para debug")
        return
    
    print(f"📊 Encontrados {len(pending_data)} processos para análise")
    
    # 2. Analisa estrutura dos primeiros 3 processos
    for i, process_data in enumerate(pending_data[:3], 1):
        processo = process_data['proceso']
        alteracoes = process_data['alteracoes']
        
        print(f"\n🔍 PROCESSO {i}: {processo}")
        
        if 'historico_alteracoes' in alteracoes:
            historico = alteracoes['historico_alteracoes']
            print(f"   📜 Histórico: {len(historico)} alterações")
            
            # Analisa campos alterados
            all_changed_fields = set()
            for change in historico:
                if 'alteracoes' in change:
                    changed_in_this_iteration = list(change['alteracoes'].keys())
                    all_changed_fields.update(changed_in_this_iteration)
                    print(f"   🔄 Mudança: {changed_in_this_iteration}")
            
            print(f"   🎯 CAMPOS QUE DEVEM SER FORMATADOS: {sorted(all_changed_fields)}")
            print(f"   📝 Tipo alteração na linha azul: {', '.join(sorted(all_changed_fields))}")
            
            # Mostra mapeamento de colunas
            mapping = {
                'porto_embarque': 'Coluna 3 (Porto)',
                'navio_embarque': 'Coluna 4 (Navio)', 
                'previsao_embarque': 'Coluna 5 (Embarque)',
                'previsao_embarque_transbordo': 'Coluna 6 (Embarque Transbordo)',
                'porto_transbordo': 'Coluna 7 (Porto Transbordo)',
                'navio_transbordo': 'Coluna 8 (Navio Transbordo)'
            }
            
            print(f"   🎨 FORMATAÇÃO SERÁ APLICADA EM:")
            for field in sorted(all_changed_fields):
                if field in mapping:
                    print(f"      • {mapping[field]} ({field})")
        
        print("   " + "-" * 40)

if __name__ == "__main__":
    debug_formatting()