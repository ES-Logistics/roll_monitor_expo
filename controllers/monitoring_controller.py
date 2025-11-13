"""
Controller principal para gerenciar o monitoramento de processos
"""
import time
from datetime import datetime
from services.monitoring_service import MonitoringService
from services.query_service import QueryService

class MonitoringController:
    
    def __init__(self):
        self.monitoring_service = MonitoringService()
        self.query_service = QueryService()
        # Campos que são monitorados para gerar relatórios
        self.MONITORED_FIELDS = ['navio_embarque', 'navio_transbordo']
    
    def show_monitoring_config(self):
        """Mostra configuração do que está sendo monitorado"""
        print("\n" + "="*70)
        print("🎯 CONFIGURAÇÃO DE MONITORAMENTO - ROLL MONITOR EXPORTAÇÃO")
        print("="*70)
        print("📊 CAMPOS MONITORADOS (geram relatório quando alterados):")
        for field in self.MONITORED_FIELDS:
            print(f"   ✅ {field}")
        print("\n🚫 CAMPOS IGNORADOS (não geram relatório):")
        ignored_fields = [
            "porto_embarque", "previsao_embarque", "porto_destino",
            "previsao_embarque_transbordo", "porto_transbordo", 
            "email_responsavel", "armador", "cliente", "motivo_transferencia"
        ]
        for field in ignored_fields:
            print(f"   ❌ {field}")
        print("="*70)
        print("💡 IMPORTANTE: Apenas mudanças em navios geram eventos de relatório!")
        print("="*70)
    
    def initialize_system(self):
        """Inicializa o sistema e as tabelas do banco"""
        self.show_monitoring_config()
        print("\n🚀 Inicializando sistema de monitoramento...")
        
        result = self.query_service.initialize_database_tables()
        
        if result['success']:
            print(result['message'])
            return True
        else:
            print(f"Erro na inicialização: {result['message']}")
            return False
    
    def compare_data(self, current_data, previous_data):
        """Compara dados atual com anterior e identifica mudanças"""
        
        print(f"\n🔍 DEBUG - Iniciando comparação:")
        print(f"   📊 Dados atuais: {len(current_data)} registros")
        print(f"   📚 Dados anteriores: {len(previous_data)} registros")
        
        # Primeiro, limpa as marcações de mudança de todos os registros atuais
        for unique_id in current_data:
            current_data[unique_id]['OBS'] = ''
            current_data[unique_id]['CHANGES_DETAIL'] = {}
        
        changes_detected = {}  # Armazena mudanças por processo
        
        # Compara apenas registros que existem em ambas as execuções
        common_records = set(current_data.keys()) & set(previous_data.keys())
        print(f"   🔗 Registros em comum para comparação: {len(common_records)}")
        
        # Debug: Vamos verificar se o processo específico está sendo comparado
        target_process = "EM09438/25"
        target_found_current = False
        target_found_previous = False
        target_unique_id = None
        
        for unique_id in current_data:
            if target_process in current_data[unique_id].get('proceso', ''):
                target_found_current = True
                target_unique_id = unique_id
                print(f"   🎯 DEBUG - Processo {target_process} encontrado nos dados atuais: {unique_id}")
                break
                
        for unique_id in previous_data:
            if target_process in previous_data[unique_id].get('proceso', ''):
                target_found_previous = True
                print(f"   🎯 DEBUG - Processo {target_process} encontrado nos dados anteriores: {unique_id}")
                break
        
        if target_found_current and target_found_previous:
            print(f"   ✅ DEBUG - Processo {target_process} será comparado")
        elif target_found_current and not target_found_previous:
            print(f"   ⚠️  DEBUG - Processo {target_process} é NOVO (não estava nos dados anteriores)")
        elif not target_found_current and target_found_previous:
            print(f"   ⚠️  DEBUG - Processo {target_process} foi REMOVIDO (não está nos dados atuais)")
        else:
            print(f"   ❌ DEBUG - Processo {target_process} NÃO encontrado em nenhum conjunto de dados")
        
        for unique_id in current_data:
            if unique_id in previous_data:
                changed_fields = []
                changes_detail = {}
                current_record = current_data[unique_id]
                previous_record = previous_data[unique_id]
                proceso = current_record['proceso']
                
                # NOVA LÓGICA: Compara APENAS campos monitorados
                # Campos que geram gatilho para relatório
                
                # Debug específico para o processo target
                is_target_process = target_process in proceso
                if is_target_process:
                    print(f"   🔍 DEBUG - Analisando processo {proceso} (unique_id: {unique_id})")
                    print(f"        Dados atuais: navio_embarque={current_record.get('navio_embarque')}, navio_transbordo={current_record.get('navio_transbordo')}")
                    print(f"        Dados anteriores: navio_embarque={previous_record.get('navio_embarque')}, navio_transbordo={previous_record.get('navio_transbordo')}")
                
                # Compara apenas os campos monitorados
                for field in self.MONITORED_FIELDS:
                    if field in current_record:
                        current_value = current_record[field]
                        previous_value = previous_record.get(field)
                        
                        # Converte None para string para comparação consistente
                        if current_value is None:
                            current_value = 'NULL'
                        if previous_value is None:
                            previous_value = 'NULL'
                        
                        if str(current_value) != str(previous_value):
                            changed_fields.append(field)
                            changes_detail[field] = {
                                'anterior': previous_value,
                                'atual': current_value
                            }
                            print(f"🔄 Mudança detectada - Processo: {proceso}, Campo: {field}, "
                                  f"Anterior: '{previous_value}' → Atual: '{current_value}'")
                        else:
                            # Debug para campos que NÃO mudaram no processo target
                            if is_target_process:
                                print(f"        ✅ Campo {field}: SEM MUDANÇA ('{previous_value}' = '{current_value}')")
                    else:
                        if is_target_process:
                            print(f"        ❌ Campo {field} não encontrado nos dados atuais")
                
                # Se houve mudanças, registra no banco e marca no dict atual
                if changed_fields:
                    current_data[unique_id]['OBS'] = ', '.join(changed_fields)
                    current_data[unique_id]['CHANGES_DETAIL'] = changes_detail
                    
                    # Agrupa mudanças por processo
                    if proceso not in changes_detected:
                        changes_detected[proceso] = {}
                    
                    # Adiciona as mudanças deste registro específico
                    for field, change_info in changes_detail.items():
                        if field not in changes_detected[proceso]:
                            changes_detected[proceso][field] = []
                        
                        changes_detected[proceso][field].append({
                            'unique_id': unique_id,
                            'anterior': change_info['anterior'],
                            'atual': change_info['atual']
                        })
        
        # Registra todas as mudanças detectadas no banco
        registered_changes = 0
        for proceso, process_changes in changes_detected.items():
            result = self.monitoring_service.register_process_changes(proceso, process_changes)
            if result['success']:
                registered_changes += 1
            else:
                print(f"Erro ao registrar mudanças do processo {proceso}: {result['message']}")
        
        # Registros novos e removidos
        new_records = set(current_data.keys()) - set(previous_data.keys())
        removed_records = set(previous_data.keys()) - set(current_data.keys())
        
        print(f"\n📋 DEBUG - Resumo da comparação:")
        print(f"   🔄 Processos com mudanças detectadas: {len(changes_detected)}")
        print(f"   📝 Mudanças registradas no banco: {registered_changes}")
        print(f"   ➕ Novos registros: {len(new_records)}")
        print(f"   ➖ Registros removidos: {len(removed_records)}")
        
        if len(changes_detected) > 0:
            print(f"   📌 Processos alterados: {list(changes_detected.keys())}")
        
        return {
            'changes_detected': len(changes_detected),
            'changes_registered': registered_changes,
            'new_records': len(new_records),
            'removed_records': len(removed_records),
            'processes_with_changes': list(changes_detected.keys())
        }
    
    def print_results(self, data, comparison_result=None):
        """Imprime processos que tiveram alterações com detalhes das mudanças"""
        print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Verificação de mudanças:")
        
        if comparison_result:
            if comparison_result['changes_detected'] > 0:
                print("=" * 80)
                print("PROCESSOS COM ALTERAÇÕES:")
                print("=" * 80)
                
                changed_count = 0
                for unique_id, info in data.items():
                    if info.get('OBS'):
                        changed_count += 1
                        proceso = info['proceso']
                        obs = info['OBS']
                        changes_detail = info.get('CHANGES_DETAIL', {})
                        
                        print(f"\n{changed_count}. Processo: {proceso}")
                        print(f"   Campos alterados: {obs}")
                        print("   Detalhes das mudanças:")
                        
                        for field, change_info in changes_detail.items():
                            anterior = change_info['anterior']
                            atual = change_info['atual']
                            print(f"     • {field}:")
                            print(f"       Anterior: {anterior}")
                            print(f"       Atual:    {atual}")
                        
                        print("-" * 60)
                
                # Resumo
                print(f"\nResumo da verificação:")
                print(f"  • Processos alterados: {comparison_result['changes_detected']}")
                print(f"  • Mudanças registradas: {comparison_result['changes_registered']}")
                print(f"  • Novos registros: {comparison_result['new_records']}")
                print(f"  • Registros removidos: {comparison_result['removed_records']}")
                
            else:
                print("Nenhuma alteração detectada.")
                if comparison_result['new_records'] > 0 or comparison_result['removed_records'] > 0:
                    print(f"Novos registros: {comparison_result['new_records']}")
                    print(f"Registros removidos: {comparison_result['removed_records']}")
        else:
            print("Nenhuma alteração detectada.")
    
    def execute_monitoring_cycle(self, max_age_hours=1):
        """Executa um ciclo completo de monitoramento"""
        
        # Executa query e obtém dados atuais
        query_result = self.query_service.execute_monitoring_query()
        
        if not query_result['success']:
            print(f"Erro ao executar query: {query_result['message']}")
            return False
        
        current_data = query_result['data']
        print(f"Dados coletados: {query_result['records_count']} registros")
        
        # Carrega dados anteriores do banco (considerando idade máxima)
        snapshot_result = self.monitoring_service.load_previous_snapshot(max_age_hours)
        previous_data = snapshot_result['data']
        
        print(f"\n🔍 DEBUG - Carregamento de snapshot:")
        print(f"   📈 Max age permitido: {max_age_hours} horas")
        print(f"   ✅ Snapshot carregado com sucesso: {snapshot_result['success']}")
        print(f"   📊 Registros no snapshot anterior: {len(previous_data)}")
        if 'message' in snapshot_result:
            print(f"   💬 Mensagem: {snapshot_result['message']}")
        
        comparison_result = None
        
        if snapshot_result['success'] and len(previous_data) > 0:
            # Compara com dados anteriores
            comparison_result = self.compare_data(current_data, previous_data)
            self.print_results(current_data, comparison_result)
        else:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Primeira execução ou dados muito antigos - {len(current_data)} processos carregados.")
        
        # Salva snapshot atual no banco (após comparações)
        save_result = self.monitoring_service.save_current_snapshot(current_data)
        if save_result['success']:
            print(f"Snapshot atualizado: {save_result['records_saved']} registros")
        else:
            print(f"Erro ao salvar snapshot: {save_result['message']}")
        
        return True
    
    def start_monitoring(self, interval_minutes=3, max_age_hours=24):
        """Inicia o loop principal de monitoramento"""
        
        if not self.initialize_system():
            return
        
        print(f"Iniciando monitoramento de processos (intervalo: {interval_minutes} minutos, idade máxima: {max_age_hours}h)...")
        
        while True:
            try:
                success = self.execute_monitoring_cycle(max_age_hours)
                
                if not success:
                    print("Erro no ciclo de monitoramento, aguardando 1 minuto...")
                    time.sleep(60)
                    continue
                
                # Aguarda o intervalo especificado
                interval_seconds = interval_minutes * 60
                print(f"Aguardando {interval_minutes} minutos para próxima verificação...")
                time.sleep(interval_seconds)
                
            except KeyboardInterrupt:
                print("\nMonitoramento interrompido pelo usuário.")
                break
            except Exception as e:
                print(f"Erro durante monitoramento: {e}")
                time.sleep(60)  # Aguarda 1 minuto antes de tentar novamente