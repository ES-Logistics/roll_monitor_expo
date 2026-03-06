import logging
import json
import config
from services.query_service import QueryService

class Monitor:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.queries = QueryService()

    def run(self):
        self.logger.info("Iniciando ciclo de monitoramento.")

        older_data = self.queries.snapshot_control()
        current_data = self.queries.run_query()

        self.logger.info("Comparando dados atuais com dados anteriores.")
        result = self.diff_by_key(older_data, current_data, 'processo', config.COLS_COMPARE)

        if result['added'] or result['removed'] or result['changed']:
            self.logger.info(f"Alterações detectadas: {json.dumps(result, indent=2, default=str, ensure_ascii=False)}")
        else:
            self.logger.info("Nenhuma alteração detectada.")

        self.queries.update_snapshot()
        self.queries.save_diffs_to_db(result["changed"])
        self.queries.remove_diffs_from_db(result["removed"])

        self.logger.info("Ciclo de monitoramento concluído.")


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
