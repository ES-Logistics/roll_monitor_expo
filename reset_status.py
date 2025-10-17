"""
Reset status dos processos para PENDENTE
"""

import psycopg2
from services.datalake_connection import get_db_config

def reset_status():
    conn = psycopg2.connect(**get_db_config())
    cursor = conn.cursor()
    cursor.execute("UPDATE bronze.roll_monitor_changes SET status_relatorio = 'PENDENTE'")
    conn.commit()
    print(f'Resetados {cursor.rowcount} processos para PENDENTE')
    cursor.close()
    conn.close()

if __name__ == "__main__":
    reset_status()