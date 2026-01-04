#!/usr/bin/env python3
"""
Setup inicial do projeto Ingestão de Dados SUS.
Cria diretórios e schemas no banco de dados.
"""
import os
import sys
import psycopg2
from pathlib import Path

# Adiciona raiz ao path para importar config
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import DB_CONFIG, PROJECT_ROOT, CACHE_DIR

def setup():
    print("="*60)
    print("🛠️  SETUP INICIAL - INGESTÃO DE DADOS SUS")
    print("="*60)
    
    # 1. Criar diretórios
    print("\n📁 Criando diretórios...")
    dirs = [
        CACHE_DIR,
        CACHE_DIR / "saude",
        CACHE_DIR / "comum",
        CACHE_DIR / "censo_renda",
        CACHE_DIR / "censo_renda" / "2010",
        CACHE_DIR / "censo_renda" / "2022",
    ]
    
    for d in dirs:
        os.makedirs(d, exist_ok=True)
        print(f"   ✅ {d}")
        
    # 2. Banco de Dados
    print(f"\nExample: Connecting to {DB_CONFIG['host']}:{DB_CONFIG['port']} ({DB_CONFIG['database']})...")
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        
        with conn.cursor() as cur:
            # Criar Schemas
            schemas = ["saude", "comum"]
            for schema in schemas:
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
                print(f"   ✅ Schema '{schema}' verificado/criado")
                
        conn.close()
        print("\n🎉 Setup concluído com sucesso!")
        print("   Agora você pode rodar os scripts em 'etl/'.")
        
    except Exception as e:
        print(f"\n❌ Erro ao conectar no Banco de Dados: {e}")
        print("   Verifique as credenciais em 'config/settings.py' ou variáveis de ambiente.")
        print(f"   Config atual: Host={DB_CONFIG['host']}, User={DB_CONFIG['user']}, DB={DB_CONFIG['database']}")

if __name__ == "__main__":
    setup()
