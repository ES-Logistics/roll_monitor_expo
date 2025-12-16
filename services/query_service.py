"""
Service para executar queries na base de dados
"""
import psycopg2
import logging

DB_CONFIG_LAKEHOUSE = {
    'host': '201.16.238.24',
    'database': 'lakehouse',
    'user': 'wellington',
    'password': 'mLeAHZxrNeYMQ54h',
}

class QueryService:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO)
        self.query1 = '''
            SELECT
                mov.ds_movimento AS processo,
                mov.dt_previsao_saida AS previsao_embarque,
                loc.nm_localidade AS porto_embarque,
                desloc.nm_localidade AS porto_destino,
                nav.nm_navio AS navio_embarque,
                tr.previsao_embarque_transbordo,
                tr.portos_transbordo AS porto_transbordo,
                tr.navios_transbordo AS navio_transbordo,
                usr.ds_email AS email_responsavel,
                usrbkn.ds_email AS email_resp_booking,
                usrcs.ds_email AS email_cs,
                pes.nm_fantasia AS armador,
                pescli.nm_fantasia AS cliente,
                mot.ds_motivo AS motivo_transferencia,
                via.ds_viagem AS viagem,
                mov.ds_reserva AS booking,
                cont.ds_quantidade_containers
            FROM bronze.skyline_es_ocs_movimento mov
            LEFT JOIN bronze.skyline_es_ocs_servico srv
                ON srv.cd_movimento = mov.cd_movimento
            LEFT JOIN bronze.skyline_es_pes_pessoa pes
                ON pes.cd_pessoa = srv.cd_fornecedor
            LEFT JOIN bronze.skyline_es_usr_usuario usr
                ON mov.cd_responsavel = usr.cd_usuario
            LEFT JOIN bronze.skyline_es_loc_localidade loc
                ON mov.cd_origem = loc.cd_localidade
            LEFT JOIN bronze.skyline_es_loc_localidade desloc
                ON mov.cd_destino = desloc.cd_localidade
            LEFT JOIN bronze.skyline_es_tab_navio nav
                ON nav.cd_navio = mov.cd_navio
            LEFT JOIN bronze.skyline_es_pes_pessoa pescli
                ON pescli.cd_pessoa = mov.cd_intermediario
            LEFT JOIN bronze.skyline_es_tab_motivo mot
                ON mot.cd_motivo = mov.cd_motivo_transferencia
            LEFT JOIN bronze.skyline_es_usr_usuario usrbkn
                ON mov.cd_responsavel_booking_desk = usrbkn.cd_usuario
            LEFT JOIN bronze.skyline_es_usr_usuario usrcs
                ON mov.cd_responsavel_internacional = usrcs.cd_usuario
            LEFT JOIN bronze.skyline_es_ocs_viagem via
                ON mov.cd_viagem = via.cd_viagem
            LEFT JOIN (
                SELECT
                    x.cd_movimento,
                    STRING_AGG(
                        x.cnt || ' X ' || x.nm_equipamento,
                        ' / '
                        ORDER BY x.nm_equipamento
                    ) AS ds_quantidade_containers
                FROM (
                    SELECT
                        em.cd_movimento,
                        e.nm_equipamento,
                        COUNT(*)::varchar AS cnt
                    FROM bronze.skyline_es_ocs_equipamento em
                    LEFT JOIN bronze.skyline_es_tab_equipamento e
                        ON e.cd_equipamento = em.cd_equipamento
                    GROUP BY em.cd_movimento, e.nm_equipamento
                ) x
                GROUP BY x.cd_movimento
            ) cont
            ON cont.cd_movimento = mov.cd_movimento
            LEFT JOIN (
                SELECT
                    trc.cd_movimento,
                    STRING_AGG(DISTINCT loctra.nm_localidade, ' / ') AS portos_transbordo,
                    STRING_AGG(DISTINCT navtra.nm_navio, ' / ') AS navios_transbordo,
                    MIN(trc.dt_previsao_saida) AS previsao_embarque_transbordo
                FROM bronze.skyline_es_ocs_trecho trc
                LEFT JOIN bronze.skyline_es_loc_localidade loctra
                    ON loctra.cd_localidade = trc.cd_localidade
                LEFT JOIN bronze.skyline_es_tab_navio navtra
                    ON navtra.cd_navio = trc.cd_navio
                GROUP BY trc.cd_movimento
            ) tr
                ON tr.cd_movimento = mov.cd_movimento
            WHERE mov.ds_movimento ILIKE 'EM%'
            AND mov.dt_previsao_saida IS NOT NULL
            AND mov.dt_confirmacao_saida IS NULL
            AND mov.dt_fechamento IS NULL
            AND srv.cd_servico = 1;
        '''
        self.query2 = '''
                        select mov.dt_confirmacao_saida  
                        from bronze.skyline_es_ocs_movimento mov
                        where mov.ds_movimento = %s'''


    def run_query(self,query=None):
        conn = psycopg2.connect(**DB_CONFIG_LAKEHOUSE)
        try:
            if query is None:
                query = self.query1
                
            with conn.cursor() as cursor:
                cursor.execute(query)
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]
        except Exception as e:
            self.logger.error(f"Erro ao executar a query: {e}")
            return []
        finally:
            conn.close()
    
    def snapshot_control(self):
        self.logger.info("Executando snapshot control.")

        #Abre conexão só pra leitura
        conn = psycopg2.connect(**DB_CONFIG_LAKEHOUSE)
        try:
            with conn.cursor() as cursor:
                cursor.execute('select * from bronze.d_roll_monitor_expo_snapshot;')
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
        finally:
            conn.close()  # <-- FECHA A CONEXÃO COMPLETAMENTE AQUI

        #Se a tabela tem dados → retorna
        if rows:
            return [dict(zip(columns, row)) for row in rows]

        #Se a tabela está vazia → atualizar snapshot
        self.update_snapshot() 

        #Carrega snapshot atualizado
        conn = psycopg2.connect(**DB_CONFIG_LAKEHOUSE)
        try:
            with conn.cursor() as cursor:
                cursor.execute('select * from bronze.d_roll_monitor_expo_snapshot;')
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
        finally:
            conn.close()

        return [dict(zip(columns, row)) for row in rows]

    def update_snapshot(self):
        conn = psycopg2.connect(**DB_CONFIG_LAKEHOUSE)
        try:
            self.logger.info("Atualizando snapshot.")
            with conn.cursor() as cursor:
                
                # pega novo snapshot
                snapshot = self.run_query()

                # se vazio, NÃO apaga a tabela
                if not snapshot:
                    self.logger.error("Snapshot retornou vazio — NÃO será atualizado para evitar perda de dados.")
                    return
                
                # limpa tabela somente se houver dados novos
                self.clear_snapshot()

                insert_query = '''
                    INSERT INTO bronze.d_roll_monitor_expo_snapshot
                    (processo, porto_embarque, navio_embarque, previsao_embarque,
                    previsao_embarque_transbordo, porto_transbordo, navio_transbordo,
                    email_responsavel, email_resp_booking, email_cs,
                    armador, cliente, motivo_transferencia, viagem, booking, ds_quantidade_containers)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                '''

                for row in snapshot:
                    cursor.execute(insert_query, (
                        row['processo'],
                        row['porto_embarque'],
                        row['navio_embarque'],
                        row['previsao_embarque'],
                        row['previsao_embarque_transbordo'],
                        row['porto_transbordo'],
                        row['navio_transbordo'],
                        row['email_responsavel'],
                        row['email_resp_booking'],
                        row['email_cs'],
                        row['armador'],
                        row['cliente'],
                        row['motivo_transferencia'],
                        row['viagem'],
                        row['booking'],
                        row['ds_quantidade_containers']
                    ))

                conn.commit()

        except Exception as e:
            self.logger.error(f"Erro ao atualizar snapshot: {e}")

        finally:
            conn.close()
    
    def clear_snapshot(self):

        conn = psycopg2.connect(**DB_CONFIG_LAKEHOUSE)
        try:
            self.logger.info("Limpando snapshot.")
            with conn.cursor() as cursor:
                cursor.execute('TRUNCATE TABLE bronze.d_roll_monitor_expo_snapshot RESTART IDENTITY CASCADE;')
                conn.commit()
        except Exception as e:
            self.logger.error(f"Erro ao limpar snapshot: {e}")
        finally:
            conn.close()

    def save_diffs_to_db(self, diffs):

        # 1️⃣ Carrega snapshot atual e cria mapa por processo
        snapshot = self.snapshot_control()

        snapshot_map = {
            row["processo"]: row
            for row in snapshot
        }

        # 2️⃣ Enriquecer cada diff com dados do snapshot
        for item in diffs:
            processo = item["key"]
            snap = snapshot_map.get(processo)

            if not snap:
                continue  # processo não existe mais, ignora

            item["armador"] = snap.get("armador")
            item["cliente"] = snap.get("cliente")
            item["email_responsavel"] = snap.get("email_responsavel")
            item["email_resp_booking"] = snap.get("email_resp_booking")
            item["email_cs"] = snap.get("email_cs")
            item["motivo_transferencia"] = snap.get("motivo_transferencia")
            item["booking"] = snap.get("booking")
            item["ds_quantidade_containers"] = snap.get("ds_quantidade_containers")
            item["porto_embarque"]= snap.get("porto_embarque")

        # 3️⃣ Buscar dt_confirmacao_saida (se existir)
        try:
            conn = psycopg2.connect(**DB_CONFIG_LAKEHOUSE)
            self.logger.info("Adicionando Confirma Embarque dos diffs.")
            with conn.cursor() as cursor:
                for item in diffs:
                    processo = item["key"]
                    cursor.execute(self.query2, (processo,))
                    result = cursor.fetchone()
                    if result:
                        item["dt_confirma"] = result[0]
            conn.commit()
        except Exception as e:
            self.logger.error(f"Erro ao adicionar Confirma Embarque aos diffs: {e}")
        finally:
            conn.close()

        # 4️⃣ Inserir diffs no banco
        try:
            conn = psycopg2.connect(**DB_CONFIG_LAKEHOUSE)
            with conn.cursor() as cursor:
                insert_query = """
                    INSERT INTO bronze.d_roll_monitor_expo_diff
                    (
                        processo,
                        armador,
                        cliente,
                        campo,
                        old_value,
                        new_value,
                        email_responsavel,
                        email_resp_booking,
                        email_cs,
                        motivo_transferencia,
                        booking,
                        ds_quantidade_containers,
                        porto_embarque
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """

                for item in diffs:
                    processo = item["key"]
                    diff_fields = item["diff"]
                    confirma = item.get("dt_confirma")

                    for campo, valores in diff_fields.items():
                        old_val = valores.get("old")
                        new_val = valores.get("new")

                        # regra de transbordo + confirmação
                        if "transbordo" in campo and confirma is None:
                            continue

                        cursor.execute(insert_query, (
                            processo,
                            item.get("armador"),
                            item.get("cliente"),
                            campo,
                            str(old_val) if old_val is not None else None,
                            str(new_val) if new_val is not None else None,
                            item.get("email_responsavel"),
                            item.get("email_resp_booking"),
                            item.get("email_cs"),
                            item.get("motivo_transferencia"),
                            item.get("booking"),
                            item.get("ds_quantidade_containers"),
                            item.get("porto_embarque")
                        ))

            conn.commit()

        except Exception as e:
            self.logger.error(f"Erro ao salvar diffs no banco: {e}")

        finally:
            conn.close()

    def mark_report_as_sent(self):
        conn = psycopg2.connect(**DB_CONFIG_LAKEHOUSE)
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE bronze.d_roll_monitor_expo_mailing
                    SET last_sent = NOW()
                """)
            conn.commit()
        finally:
            conn.close()

    def remove_diffs_from_db(self, processos):
        conn = psycopg2.connect(**DB_CONFIG_LAKEHOUSE)
        try:
            with conn.cursor() as cursor:
                delete_query = """
                    DELETE FROM bronze.d_roll_monitor_expo_diff
                    WHERE processo = %s
                """

                for proceso in processos:
                    cursor.execute(delete_query, (proceso,))

            conn.commit()
        except Exception as e:
            self.logger.error(f"Erro ao remover diffs do banco: {e}")
        finally:
            conn.close()

    def get_diffs_from_db(self):
        conn = psycopg2.connect(**DB_CONFIG_LAKEHOUSE)
        try:
            with conn.cursor() as cursor:
                cursor.execute('SELECT * FROM bronze.d_roll_monitor_expo_diff;')
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in rows]
        except Exception as e:
            self.logger.error(f"Erro ao buscar diffs do banco: {e}")
            return []
        finally:
            conn.close()   