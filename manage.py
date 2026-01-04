#!/usr/bin/env python3
"""
CLI Unificada para Ingestão de Dados.
Gerencia a execução dos pipelines de ETL e define o destino (Banco de Dados ou CSV).

Uso:
    python manage.py --help
    python manage.py --sources internacoes cnefe --target postgres
    python manage.py --sources estabelecimentos --target csv
"""
import argparse
import sys
import os
import time
from datetime import datetime, timedelta
from pathlib import Path

# Adiciona raiz ao path
sys.path.insert(0, str(Path(__file__).parent))

from config.settings import ETL_CONFIG, PROJECT_ROOT

# Mapeamento de fontes para módulos/funções
# Chave: nome na linha de comando/config
# Valor: (modulo, funcao_principal)
ETL_MODULES = {
    "internacoes": ("etl.saude.internacoes", "baixar_e_processar"),
    "estabelecimentos": ("etl.saude.estabelecimentos", "baixar_e_processar"),
    "cnefe": ("etl.comum.cnefe", "baixar_e_processar"),
    "censo_renda": ("etl.comum.setor_renda", "baixar_e_processar_censo"),
    "ipca": ("etl.comum.ipca", "carregar_ipca"),
    "municipios": ("etl.comum.municipios", "carregar_municipios"),
    "cid10": ("etl.saude.cid10", "carregar_cid10"),
}


def formatar_duracao(segundos):
    """Formata segundos em formato legível (1h 23m 45s)."""
    if segundos < 60:
        return f"{segundos:.1f}s"
    elif segundos < 3600:
        minutos = int(segundos // 60)
        segs = int(segundos % 60)
        return f"{minutos}m {segs}s"
    else:
        horas = int(segundos // 3600)
        minutos = int((segundos % 3600) // 60)
        segs = int(segundos % 60)
        return f"{horas}h {minutos}m {segs}s"


def main():
    parser = argparse.ArgumentParser(description="Gerenciador de ETLs do Projeto")
    
    parser.add_argument(
        "--sources", "-s", 
        nargs="+", 
        choices=list(ETL_MODULES.keys()) + ["all"],
        default=["all"],
        help="Quais fontes processar (default: todas)"
    )
    
    parser.add_argument(
        "--target", "-t",
        choices=["postgres", "csv"],
        default="postgres",
        help="Destino dos dados: 'postgres' (Banco de Dados) ou 'csv' (Arquivos locais em output/)"
    )
    
    args = parser.parse_args()
    
    # Define fontes a executar
    sources_to_run = []
    if "all" in args.sources:
        # Ordem de execução sugerida (municipios antes do cnefe para garantir tabela de lookup)
        # cid10 e ipca são lookups rápidos, podem rodar no início
        sources_to_run = ["ipca", "cid10", "municipios", "estabelecimentos", "internacoes", "censo_renda", "cnefe"]
    else:
        sources_to_run = args.sources
    
    # Timestamp de início
    inicio_pipeline = time.time()
    inicio_dt = datetime.now()
        
    print(f"🚀 Iniciando Pipeline de Dados")
    print(f"🕐 Início: {inicio_dt.strftime('%d/%m/%Y %H:%M:%S')}")
    print(f"🎯 Target: {args.target.upper()}")
    print(f"📦 Sources: {', '.join(sources_to_run)}")
    print("=" * 60)
    
    # Cria pasta output se for CSV
    if args.target == "csv":
        output_dir = PROJECT_ROOT / "output"
        output_dir.mkdir(exist_ok=True)
        print(f"📂 Diretório de saída: {output_dir}")
    
    import importlib
    
    success_count = 0
    fail_count = 0
    
    # Armazena tempos de cada fonte
    tempos = {}
    
    for source in sources_to_run:
        if source not in ETL_MODULES:
            print(f"⚠️  Fonte desconhecida: {source}")
            continue
            
        module_name, func_name = ETL_MODULES[source]
        
        print(f"\n{'='*60}")
        print(f"▶️  Executando: {source.upper()}")
        print(f"{'='*60}")
        
        inicio_fonte = time.time()
        
        try:
            module = importlib.import_module(module_name)
            func = getattr(module, func_name)
            
            # Chama a função passando o target
            import inspect
            sig = inspect.signature(func)
            
            if 'tipo_saida' in sig.parameters:
                func(tipo_saida=args.target)
            else:
                if args.target == 'csv':
                    print(f"⚠️  {source} ainda não suporta exportação CSV. Rodando padrão...")
                func()
            
            fim_fonte = time.time()
            duracao = fim_fonte - inicio_fonte
            tempos[source] = {"duracao": duracao, "status": "✅"}
            
            print(f"\n✅ {source} finalizado com sucesso")
            print(f"⏱️  Tempo: {formatar_duracao(duracao)}")
            success_count += 1
            
        except Exception as e:
            fim_fonte = time.time()
            duracao = fim_fonte - inicio_fonte
            tempos[source] = {"duracao": duracao, "status": "❌"}
            
            print(f"❌ Erro ao rodar {source}: {e}")
            import traceback
            traceback.print_exc()
            print(f"⏱️  Tempo até falha: {formatar_duracao(duracao)}")
            fail_count += 1
    
    # Fim do pipeline
    fim_pipeline = time.time()
    duracao_total = fim_pipeline - inicio_pipeline
    fim_dt = datetime.now()
    
    # Resumo final
    print("\n" + "=" * 60)
    print("📊 RESUMO DO PIPELINE")
    print("=" * 60)
    
    # Tabela de tempos por fonte
    print("\n⏱️  TEMPO POR FONTE:")
    print("-" * 40)
    for source, info in tempos.items():
        status = info["status"]
        tempo_fmt = formatar_duracao(info["duracao"])
        print(f"   {status} {source.ljust(20)} {tempo_fmt.rjust(12)}")
    print("-" * 40)
    
    # Totais
    print(f"\n🕐 Início:    {inicio_dt.strftime('%d/%m/%Y %H:%M:%S')}")
    print(f"🕐 Fim:       {fim_dt.strftime('%d/%m/%Y %H:%M:%S')}")
    print(f"⏱️  TOTAL:     {formatar_duracao(duracao_total)}")
    print(f"\n🏁 Resultado: {success_count} sucesso(s) | {fail_count} falha(s)")
    print("=" * 60)

if __name__ == "__main__":
    main()
