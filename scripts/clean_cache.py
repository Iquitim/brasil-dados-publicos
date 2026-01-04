#!/usr/bin/env python3
"""
Script de Limpeza de Cache.
Calcula o espaço usado pela pasta 'cache/' e permite deletar arquivos temporários.

Uso:
    python clean_cache.py          -> Apenas mostra tamanho
    python clean_cache.py --clean  -> Executa limpeza (pede confirmação)
    python clean_cache.py --force  -> Executa limpeza (sem confirmação)
"""
import sys
import os
import math
import shutil
import argparse
from pathlib import Path

# Adiciona raiz ao path para importar config
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import CACHE_DIR

def calcular_tamanho(path):
    """Calcula tamanho total do diretório em bytes."""
    total_size = 0
    if not path.exists():
        return 0
    
    for dirpath, dirnames, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            try:
                # Evita erro em links simbólicos quebrados
                total_size += os.path.getsize(fp)
            except Exception:
                pass
    return total_size

def formatar_tamanho(size_bytes):
    """Formata bytes para MB/GB."""
    if size_bytes == 0:
        return "0 B"
    size_name = ("B", "KB", "MB", "GB", "TB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return f"{s} {size_name[i]}"

def limpar_cache(dry_run=False):
    """Remove conteúdo da pasta cache."""
    if not CACHE_DIR.exists():
        print("✅ Cache já está vazio (diretório não existe).")
        return

    tamanho = calcular_tamanho(CACHE_DIR)
    tamanho_fmt = formatar_tamanho(tamanho)
    
    print(f"📦 Diretório de Cache: {CACHE_DIR}")
    print(f"📊 Tamanho Ocupado: {tamanho_fmt}")
    
    if tamanho == 0:
        print("✅ Cache já está vazio.")
        return

    if dry_run:
        print("\n💡 Para limpar, rode: python clean_cache.py --clean")
        return

    # Confirmação
    print("\n⚠️  ATENÇÃO: Isso apagará TODOS os arquivos baixados (CSVs, ZIPs, Parquets).")
    print("   Se você rodar o ETL novamente, tudo será baixado de novo.")
    resp = input(f"   Deseja apagar {tamanho_fmt}? [S/n]: ")
    
    if resp.lower() not in ['s', 'y', 'sim', 'yes', '']:
        print("❌ Operação cancelada.")
        return

    print("\n🧹 Limpando...")
    try:
        # Remove todo o diretório e recria
        shutil.rmtree(CACHE_DIR)
        os.makedirs(CACHE_DIR, exist_ok=True)
        print("✨ Limpeza concluída com sucesso!")
    except Exception as e:
        print(f"❌ Erro ao limpar cache: {e}")

def main():
    parser = argparse.ArgumentParser(description="Gerenciador de Cache do Projeto")
    parser.add_argument("--clean", action="store_true", help="Executa a limpeza")
    parser.add_argument("--force", action="store_true", help="Força limpeza sem perguntar")
    
    args = parser.parse_args()
    
    if args.force:
        # Hack para bypassar input na função limpar_cache se force=True
        # Mas vamos refazer a lógica pra ser mais limpo
        if CACHE_DIR.exists():
             shutil.rmtree(CACHE_DIR)
             os.makedirs(CACHE_DIR, exist_ok=True)
             print("✨ Cache limpo (forçado).")
    elif args.clean:
        limpar_cache(dry_run=False)
    else:
        # Default: Apenas mostra tamanho
        limpar_cache(dry_run=True)

if __name__ == "__main__":
    main()
