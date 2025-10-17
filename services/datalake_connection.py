import psycopg2
import pandas as pd
from sqlalchemy import create_engine

# Configuração padrão de conexão
def get_db_config():
    return {
        'host': '201.16.238.24',
        'database': 'lakehouse',
        'user': 'wellington',
        'password': 'mLeAHZxrNeYMQ54h',
        'port': '5432'
    }

def get_connection_string():
    """Retorna string de conexão para SQLAlchemy"""
    config = get_db_config()
    return f"postgresql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"

def get_sqlalchemy_engine():
    """Retorna engine SQLAlchemy para uso com pandas"""
    return create_engine(get_connection_string())
