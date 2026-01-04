#!/usr/bin/env python3
"""
Script para Limpar/Resetar o Banco de Dados.
Remove os schemas 'saude' e 'comum' e todos os dados para permitir testes do zero.

Uso:
    python reset_database.py          -> Mostra o que será apagado
    python reset_database.py --reset  -> Executa o reset (pede confirmação)
    python reset_database.py --force  -> Executa o reset (sem confirmação)
"""
import sys
import argparse
import psycopg2
from pathlib import Path

# Adiciona raiz ao path para importar config
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import DB_CONFIG

SCHEMAS_TO_DROP = ["saude", "comum"]


def get_database_info():
    """Obtém informações sobre o banco de dados (tabelas, tamanho)."""
    info = {"schemas": [], "total_tables": 0, "total_size": "0 bytes"}
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            # Verifica quais schemas existem
            for schema in SCHEMAS_TO_DROP:
                cur.execute("""
                    SELECT schema_name 
                    FROM information_schema.schemata 
                    WHERE schema_name = %s
                """, (schema,))
                if cur.fetchone():
                    # Conta tabelas no schema
                    cur.execute("""
                        SELECT COUNT(*) 
                        FROM information_schema.tables 
                        WHERE table_schema = %s
                    """, (schema,))
                    table_count = cur.fetchone()[0]
                    
                    # Calcula tamanho do schema
                    cur.execute("""
                        SELECT COALESCE(pg_size_pretty(SUM(pg_total_relation_size(quote_ident(schemaname) || '.' || quote_ident(tablename)))), '0 bytes')
                        FROM pg_tables
                        WHERE schemaname = %s
                    """, (schema,))
                    size = cur.fetchone()[0]
                    
                    info["schemas"].append({
                        "name": schema,
                        "tables": table_count,
                        "size": size
                    })
                    info["total_tables"] += table_count
            
            # Tamanho total dos schemas
            if info["schemas"]:
                cur.execute("""
                    SELECT COALESCE(pg_size_pretty(SUM(pg_total_relation_size(quote_ident(schemaname) || '.' || quote_ident(tablename)))), '0 bytes')
                    FROM pg_tables
                    WHERE schemaname IN %s
                """, (tuple(SCHEMAS_TO_DROP),))
                info["total_size"] = cur.fetchone()[0]
                
        conn.close()
    except Exception as e:
        print(f"❌ Erro ao conectar no banco: {e}")
        return None
    
    return info


def reset_database(dry_run=False, force=False):
    """Remove os schemas e todos os dados."""
    
    print(f"🗄️  Banco de Dados: {DB_CONFIG['database']}@{DB_CONFIG['host']}")
    print(f"👤 Usuário: {DB_CONFIG['user']}")
    print("=" * 60)
    
    info = get_database_info()
    if info is None:
        return
    
    if not info["schemas"]:
        print("✅ Nenhum schema encontrado para limpar. Banco já está vazio.")
        return
    
    print("\n📊 DADOS QUE SERÃO APAGADOS:")
    print("-" * 40)
    for schema in info["schemas"]:
        print(f"   📁 Schema '{schema['name']}':")
        print(f"      • Tabelas: {schema['tables']}")
        print(f"      • Tamanho: {schema['size']}")
    print("-" * 40)
    print(f"   📦 TOTAL: {info['total_tables']} tabelas | {info['total_size']}")
    
    if dry_run:
        print("\n💡 Para resetar, rode: python reset_database.py --reset")
        return
    
    # Confirmação
    if not force:
        print("\n⚠️  ATENÇÃO: Isso apagará TODOS os dados dos schemas 'saude' e 'comum'!")
        print("   Todas as tabelas, views, funções e dados serão perdidos.")
        print("   Você precisará rodar 'python manage.py' para recriar tudo.\n")
        resp = input(f"   Deseja apagar {info['total_size']} de dados? [S/n]: ")
        
        if resp.lower() not in ['s', 'y', 'sim', 'yes', '']:
            print("❌ Operação cancelada.")
            return
    
    # Executa o DROP
    print("\n🧹 Removendo schemas...")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        
        with conn.cursor() as cur:
            for schema in SCHEMAS_TO_DROP:
                print(f"   🗑️  Apagando schema '{schema}'...")
                cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
                print(f"   ✅ Schema '{schema}' removido")
        
        conn.close()
        print("\n✨ Reset concluído com sucesso!")
        print("   Agora você pode rodar:")
        print("   1. python setup_project.py  -> Recria os schemas")
        print("   2. python manage.py         -> Carrega os dados")
        
    except Exception as e:
        print(f"❌ Erro ao resetar banco: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Limpa o banco de dados para testes do zero",
        epilog="Exemplo: python reset_database.py --reset"
    )
    parser.add_argument(
        "--reset", 
        action="store_true", 
        help="Executa o reset (pede confirmação)"
    )
    parser.add_argument(
        "--force", 
        action="store_true", 
        help="Força o reset sem pedir confirmação"
    )
    
    args = parser.parse_args()
    
    if args.force:
        reset_database(dry_run=False, force=True)
    elif args.reset:
        reset_database(dry_run=False, force=False)
    else:
        # Default: Apenas mostra o que será apagado
        reset_database(dry_run=True)


if __name__ == "__main__":
    main()
