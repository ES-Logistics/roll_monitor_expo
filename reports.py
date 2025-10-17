"""
Script utilitário para consultar alterações pendentes de relatório
"""
from services.monitoring_service import MonitoringService
import json
from datetime import datetime

def show_pending_changes():
    """Mostra todas as alterações pendentes de relatório"""
    
    service = MonitoringService()
    
    print("=" * 80)
    print("RELATÓRIO DE ALTERAÇÕES PENDENTES")
    print("=" * 80)
    
    result = service.get_pending_reports()
    
    if not result['success']:
        print(f"Erro ao buscar alterações: {result['message']}")
        return
    
    pending_changes = result['data']
    
    if not pending_changes:
        print("Nenhuma alteração pendente encontrada.")
        return
    
    print(f"Total de processos com alterações pendentes: {len(pending_changes)}")
    print()
    
    for i, change in enumerate(pending_changes, 1):
        processo = change['proceso']
        alteracoes = change['alteracoes']
        created_at = change['created_at']
        updated_at = change['updated_at']
        
        print(f"{i}. Processo: {processo}")
        print(f"   Primeira detecção: {alteracoes.get('primeira_deteccao', 'N/A')}")
        print(f"   Última alteração: {alteracoes.get('ultima_alteracao', 'N/A')}")
        print(f"   Total de alterações: {len(alteracoes.get('historico_alteracoes', []))}")
        
        # Mostra as últimas 3 alterações
        historico = alteracoes.get('historico_alteracoes', [])
        if historico:
            print("   Últimas alterações:")
            for j, alt in enumerate(historico[-3:], 1):
                timestamp = alt.get('timestamp', 'N/A')
                print(f"     {j}. {timestamp}")
                
                for campo, detalhes in alt.get('alteracoes', {}).items():
                    print(f"        • {campo}: {len(detalhes)} registro(s) alterado(s)")
        
        print("-" * 60)

def mark_all_as_reported():
    """Marca todas as alterações como enviadas"""
    
    service = MonitoringService()
    
    # Busca alterações pendentes
    result = service.get_pending_reports()
    
    if not result['success']:
        print(f"Erro ao buscar alterações: {result['message']}")
        return
    
    pending_changes = result['data']
    
    if not pending_changes:
        print("Nenhuma alteração pendente encontrada.")
        return
    
    processos_list = [change['proceso'] for change in pending_changes]
    
    print(f"Marcando {len(processos_list)} processos como enviados...")
    
    # Marca como enviado
    mark_result = service.mark_reports_as_sent(processos_list)
    
    if mark_result['success']:
        print(f"✓ {mark_result['updated_count']} processos marcados como enviados com sucesso!")
    else:
        print(f"✗ Erro: {mark_result['message']}")

def show_process_details(proceso):
    """Mostra detalhes completos de um processo específico"""
    
    service = MonitoringService()
    
    result = service.get_process_changes(proceso)
    
    if not result['success']:
        print(f"Erro: {result['message']}")
        return
    
    if not result['data']:
        print(f"Processo '{proceso}' não encontrado ou não possui alterações.")
        return
    
    change_data = result['data']
    alteracoes = change_data['alteracoes']
    
    print("=" * 80)
    print(f"DETALHES DO PROCESSO: {proceso}")
    print("=" * 80)
    
    print(f"Status do relatório: {change_data['status_relatorio']}")
    print(f"Criado em: {change_data['created_at']}")
    print(f"Atualizado em: {change_data['updated_at']}")
    print(f"Primeira detecção: {alteracoes.get('primeira_deteccao', 'N/A')}")
    print(f"Última alteração: {alteracoes.get('ultima_alteracao', 'N/A')}")
    
    historico = alteracoes.get('historico_alteracoes', [])
    print(f"\nTotal de alterações: {len(historico)}")
    
    if historico:
        print("\nHistórico completo de alterações:")
        for i, alt in enumerate(historico, 1):
            timestamp = alt.get('timestamp', 'N/A')
            print(f"\n{i}. {timestamp}")
            
            for campo, detalhes in alt.get('alteracoes', {}).items():
                print(f"   • Campo: {campo}")
                for detalhe in detalhes:
                    print(f"     - ID: {detalhe.get('unique_id', 'N/A')}")
                    print(f"       Anterior: {detalhe.get('anterior', 'N/A')}")
                    print(f"       Atual: {detalhe.get('atual', 'N/A')}")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == "pending":
            show_pending_changes()
        elif command == "mark-sent":
            mark_all_as_reported()
        elif command == "process" and len(sys.argv) > 2:
            proceso = sys.argv[2]
            show_process_details(proceso)
        else:
            print("Uso:")
            print("  python reports.py pending           - Mostra alterações pendentes")
            print("  python reports.py mark-sent         - Marca todas como enviadas")
            print("  python reports.py process <PROC>    - Detalhes de um processo")
    else:
        show_pending_changes()