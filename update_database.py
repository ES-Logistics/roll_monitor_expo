"""
Script para truncar tabelas e recriar com novos campos
"""

import psycopg2
from services.datalake_connection import get_db_config

def recreate_tables():
    """Trunca tabelas existentes e recria com novos campos"""
    
    print("🗄️  ATUALIZANDO ESTRUTURA DO BANCO DE DADOS")
    print("=" * 50)
    
    conn = psycopg2.connect(**get_db_config())
    
    try:
        cursor = conn.cursor()
        
        # 1. Trunca tabelas existentes
        print("🧹 Limpando tabelas existentes...")
        cursor.execute("TRUNCATE bronze.roll_monitor_snapshot RESTART IDENTITY CASCADE")
        cursor.execute("TRUNCATE bronze.roll_monitor_changes RESTART IDENTITY CASCADE")
        
        # 2. Adiciona novas colunas se não existem
        print("📋 Adicionando novas colunas...")
        
        # Verifica e adiciona email_responsavel
        try:
            cursor.execute("ALTER TABLE bronze.roll_monitor_snapshot ADD COLUMN email_responsavel VARCHAR(150)")
            print("   ✅ Coluna 'email_responsavel' adicionada")
        except:
            print("   ℹ️  Coluna 'email_responsavel' já existe")
        
        # Verifica e adiciona armador
        try:
            cursor.execute("ALTER TABLE bronze.roll_monitor_snapshot ADD COLUMN armador VARCHAR(200)")
            print("   ✅ Coluna 'armador' adicionada")
        except:
            print("   ℹ️  Coluna 'armador' já existe")
        
        # Verifica e adiciona cliente
        try:
            cursor.execute("ALTER TABLE bronze.roll_monitor_snapshot ADD COLUMN cliente VARCHAR(200)")
            print("   ✅ Coluna 'cliente' adicionada")
        except:
            print("   ℹ️  Coluna 'cliente' já existe")
        
        conn.commit()
        
        print("\n✅ Estrutura do banco atualizada com sucesso!")
        print("   📊 Tabelas limpas e prontas para novos dados")
        print("   🔗 Novos campos: cliente, armador, email_responsavel")
        
    except Exception as e:
        print(f"❌ Erro ao atualizar banco: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    recreate_tables()