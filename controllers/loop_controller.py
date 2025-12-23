import logging
import time
import json
import config
from services.mailing_serivce import MailReport
from services.query_service import QueryService

class Loop:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO)
        self.queries= QueryService()
        self.report = MailReport()

        
    def start_loop(self):
        self.logger.info("Loop iniciado.")
        
        current_data = []
        older_data = []
        try:
            while True:
                self.logger.info("Executando iteração do loop.")
                if older_data == []:
                    older_data = self.queries.snapshot_control()
                    current_data = older_data
                else:
                    current_data = self.queries.run_query()
                
                #chama comparação
                self.logger.info("Comparando dados atuais com dados anteriores.")
                result =self.diff_by_key(older_data, current_data, 'processo', config.COLS_COMPARE)
                
                #printa comparação se houver mudanças
                if result['added'] or result['removed'] or result['changed']:
                    self.logger.info(f"Alterações detectadas: {json.dumps(result, indent=2, default=str, ensure_ascii=False)}")
                    #atualiza older_data apenas se tiver mudança, se não não precisa
                else:
                    self.logger.info("Nenhuma alteração detectada.")
                
                #atualiza snapshot e older_data
                older_data = current_data
                self.queries.update_snapshot()
                
                #adiciona mudanças ao banco
                self.queries.save_diffs_to_db(result["changed"])

                #remove os processos que sumiram do monitoramento
                self.queries.remove_diffs_from_db(result["removed"])
                if self.report.should_send_report():
                    self.logger.info("[LOOP] Hora de enviar relatório de email.")
                    self.report.send()
                    self.queries.mark_report_as_sent()
                else:
                    self.logger.info("[LOOP] Ainda não é hora de enviar email.")
                self.logger.info("Iteração do loop concluída. Aguardando próxima execução.")
                time.sleep(config.MONITORING_INTERVAL_SECONDS)  

        except Exception as e:
            self.logger.error(f"Erro no loop: {e}")
        except KeyboardInterrupt:
            self.logger.info("Loop interrompido pelo usuário.")
        finally:
            self.logger.info("Loop finalizado.")


    def diff_by_key(self, old_list, new_list, key, cols):
        """
        Compara duas listas de dicts usando uma chave única.
        - old_list: estado anterior
        - new_list: estado atual
        - key: chave única (string)
        - cols: colunas a comparar (lista de strings)
        """

        # indexa por chave para lookup O(1)
        old_map = {item[key]: item for item in old_list if key in item}
        new_map = {item[key]: item for item in new_list if key in item}

        diffs = {
            "changed": [],   # itens que mudaram em alguma coluna
            "removed": [],   # itens que existiam antes e sumiram
            "added": []      # itens novos
        }

        # todos os pedidos possíveis
        all_keys = set(old_map.keys()) | set(new_map.keys())

        for k in all_keys:
            old = old_map.get(k)
            new = new_map.get(k)

            # caso 1 — existia antes e sumiu
            if old and not new:
                diffs["removed"].append(k)
                continue

            # caso 2 — novo pedido
            if new and not old:
                diffs["added"].append(k)
                continue

            # caso 3 — existe nos dois → comparar colunas
            row_diff = {}
            for c in cols:
                v_old = old.get(c)
                v_new = new.get(c)
                if v_old != v_new:
                    row_diff[c] = {
                        "old": v_old,
                        "new": v_new
                    }
            #verificar se a mudança é de previsão ou previsão transbordo, se for, só entra se "motivo_alteracao" tiver preenchido, se não, ignora
            if row_diff:
                if "previsao_embarque" in row_diff or "previsao_embarque_transbordo" in row_diff:
                    motivo = new.get("motivo_alteracao")
                    if motivo and motivo.strip() != "":
                        diffs["changed"].append({
                            "key": k,
                            "diff": row_diff
                        })
                else:
                    diffs["changed"].append({
                        "key": k,
                        "diff": row_diff
                    })
        return diffs
