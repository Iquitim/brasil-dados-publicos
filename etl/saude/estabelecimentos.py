"""
Script para carregar/atualizar tabela de estabelecimentos de saúde (CNES).
Inclui endereço completo com CEP dos hospitais.

O código CNES é a chave estrangeira para a tabela de internações (codigo_estabelecimento).

Uso:
    source venv/bin/activate
    python -m etl.saude.estabelecimentos
"""

import os
import shutil
from pathlib import Path
from pysus.ftp.databases.cnes import CNES
import pandas as pd
import psycopg2
from tqdm import tqdm
from io import StringIO
import sys
from pathlib import Path
from datetime import date

# Adiciona raiz do projeto ao path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dicionarios.cnes import (
    obter_nome_intuitivo,
    obter_descricao,
    obter_mapeamento_colunas,
    obter_dicionario_completo
)

# --- CONFIGURAÇÕES ---
# Conexão PostgreSQL e Configuração Modular
from config.settings import DB_CONFIG, ETL_CONFIG

# Obtém configs específicas ou usa defaults
CONF = ETL_CONFIG.get("estabelecimentos", {})
UFS = CONF.get("ufs", ETL_CONFIG["padrao"]["ufs"])
ANO_INICIO = CONF.get("ano_inicio", 2024)
MES_INICIO = CONF.get("mes_inicio", 1)
ANO_FIM = CONF.get("ano_fim", 2024)
MES_FIM = CONF.get("mes_fim", 12)

PASTA_CACHE = "cache/cnes"
SCHEMA = "saude"
TABELA = "estabelecimentos"


def renomear_colunas(df):
    """Renomeia colunas do DataFrame para nomes intuitivos."""
    mapeamento = obter_mapeamento_colunas()
    novas_colunas = {}
    for col in df.columns:
        col_lower = col.lower()
        if col_lower in mapeamento:
            novas_colunas[col] = mapeamento[col_lower]
        else:
            novas_colunas[col] = col_lower
    return df.rename(columns=novas_colunas)


def mapear_tipo_postgres(dtype):
    """Mapeia tipos do pandas para PostgreSQL."""
    dtype_str = str(dtype).lower()
    if 'int' in dtype_str:
        return 'BIGINT'
    elif 'float' in dtype_str:
        return 'DOUBLE PRECISION'
    elif 'bool' in dtype_str:
        return 'BOOLEAN'
    elif 'datetime' in dtype_str or 'date' in dtype_str:
        return 'TIMESTAMP'
    else:
        return 'TEXT'


def tabela_existe(conn, schema, tabela):
    """Verifica se a tabela existe."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = %s AND table_name = %s
            )
        """, (schema, tabela))
        return cur.fetchone()[0]


def obter_colunas_tabela(conn, schema, tabela):
    """Retorna set de colunas existentes na tabela."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = %s AND table_name = %s
        """, (schema, tabela))
        return {row[0].lower() for row in cur.fetchall()}


def criar_tabela_dicionario(conn, schema):
    """Cria/atualiza a tabela de dicionário de colunas do CNES."""
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {schema}.dicionario_colunas (
                id SERIAL PRIMARY KEY,
                tabela TEXT NOT NULL,
                coluna TEXT NOT NULL,
                nome_original_datasus TEXT,
                descricao TEXT,
                tipo TEXT,
                exemplo TEXT,
                UNIQUE(tabela, coluna)
            );
        """)
        
        # Insere dados do dicionário CNES
        dicionario = obter_dicionario_completo()
        for nome_original, info in dicionario.items():
            nome_intuitivo = info.get('nome_intuitivo', nome_original)
            cur.execute(f"""
                INSERT INTO {schema}.dicionario_colunas 
                    (tabela, coluna, nome_original_datasus, descricao, tipo, exemplo)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (tabela, coluna) DO UPDATE SET
                    nome_original_datasus = EXCLUDED.nome_original_datasus,
                    descricao = EXCLUDED.descricao,
                    tipo = EXCLUDED.tipo,
                    exemplo = EXCLUDED.exemplo
            """, (
                'estabelecimentos',
                nome_intuitivo,
                nome_original.upper(),
                info.get('descricao', ''),
                info.get('tipo', ''),
                info.get('exemplo', '')
            ))
        
        conn.commit()
        print(f"   ✅ Dicionário atualizado com {len(dicionario)} colunas do CNES")


def adicionar_comments_colunas(conn, schema, tabela):
    """Adiciona COMMENT em cada coluna."""
    colunas = obter_colunas_tabela(conn, schema, tabela)
    dicionario = obter_dicionario_completo()
    
    mapeamento_inverso = {info.get('nome_intuitivo', k): k for k, info in dicionario.items()}
    
    with conn.cursor() as cur:
        for coluna in colunas:
            nome_original = mapeamento_inverso.get(coluna, coluna)
            if nome_original in dicionario:
                info = dicionario[nome_original]
                descricao = f"{info.get('descricao', '')} [DATASUS: {nome_original.upper()}]"
            else:
                descricao = f"Coluna do CNES"
            
            descricao_escaped = descricao.replace("'", "''")
            try:
                cur.execute(f"""
                    COMMENT ON COLUMN {schema}.{tabela}."{coluna}" IS '{descricao_escaped}'
                """)
            except Exception:
                pass
        conn.commit()


def criar_tabela_inicial(conn, df, schema, tabela):
    """Cria a tabela com suporte a histórico (competencia na PK)."""
    with conn.cursor() as cur:
        # Colunas básicas
        colunas_sql = [
            '"codigo_cnes" TEXT NOT NULL',  # Parte da PK
            '"competencia" DATE NOT NULL',  # Parte da PK (YYYY-MM-01)
            '"uf" CHAR(2) NOT NULL',
            '"arquivo_origem" TEXT NOT NULL',
            '"data_carga" TIMESTAMP DEFAULT CURRENT_TIMESTAMP'
        ]
        
        for col in df.columns:
            col_lower = col.lower()
            if col_lower not in ['codigo_cnes', 'competencia', 'uf', 'arquivo_origem']:
                tipo = mapear_tipo_postgres(df[col].dtype)
                colunas_sql.append(f'"{col_lower}" {tipo}')
        
        # Primary Key Composta: CNES + Competencia
        create_sql = f'''
            CREATE TABLE {schema}.{tabela} (
                {", ".join(colunas_sql)},
                PRIMARY KEY (codigo_cnes, competencia)
            )
        '''
        cur.execute(create_sql)
        
        # Comentário da Tabela
        cur.execute(f"""
            COMMENT ON TABLE {schema}.{tabela} IS 
                'Histórico de Estabelecimentos do CNES. Chave Primária: (codigo_cnes, competencia).
                 Permite analisar mudanças de endereço ou características do hospital ao longo do tempo.'
        """)
        
        # Índices Adicionais
        cur.execute(f'CREATE INDEX idx_{tabela}_comp ON {schema}.{tabela}(competencia)')
        cur.execute(f'CREATE INDEX idx_{tabela}_uf ON {schema}.{tabela}(uf)')
        cur.execute(f'CREATE INDEX idx_{tabela}_municipio ON {schema}.{tabela}(codigo_municipio)')
        cur.execute(f'CREATE INDEX idx_{tabela}_cep ON {schema}.{tabela}(cep)')
        
        conn.commit()
        print(f"   ✅ Tabela Histórica {schema}.{tabela} criada (PK: codigo_cnes + competencia)")
        
        adicionar_comments_colunas(conn, schema, tabela)
        criar_tabela_dicionario(conn, schema)


def atualizar_schema_tabela(conn, df, schema, tabela):
    """Adiciona novas colunas se surgirem."""
    colunas_existentes = obter_colunas_tabela(conn, schema, tabela)
    
    with conn.cursor() as cur:
        for col in df.columns:
            col_lower = col.lower()
            if col_lower not in colunas_existentes:
                tipo = mapear_tipo_postgres(df[col].dtype)
                cur.execute(f'ALTER TABLE {schema}.{tabela} ADD COLUMN IF NOT EXISTS "{col_lower}" {tipo}')
        conn.commit()


def inserir_dados(conn, df, schema, tabela, arquivo, uf, competencia):
    """Insere dados de forma idempotente (apaga competência antes de inserir)."""
    df = df.copy()
    df['arquivo_origem'] = arquivo
    df['uf'] = uf
    df['competencia'] = competencia # Data reference
    df.columns = [c.lower() for c in df.columns]
    
    # Garantir schema atualizado
    if not tabela_existe(conn, schema, tabela):
        criar_tabela_inicial(conn, df, schema, tabela)
    else:
        atualizar_schema_tabela(conn, df, schema, tabela)
    
    # 1. Limpar competência existente (Idempotência)
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {schema}.{tabela} WHERE competencia = %s AND uf = %s", (competencia, uf))
    conn.commit()
    
    # 2. Preparar COPY
    colunas_tabela = obter_colunas_tabela(conn, schema, tabela)
    colunas_tabela = colunas_tabela - {'data_carga'} # Gerado pelo banco
    
    colunas_insert = [c for c in df.columns if c in colunas_tabela]
    df_insert = df[colunas_insert]
    
    buffer = StringIO()
    df_insert.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N')
    buffer.seek(0)
    
    with conn.cursor() as cur:
        cols_quoted = ', '.join([f'"{c}"' for c in colunas_insert])
        cur.copy_expert(
            f"COPY {schema}.{tabela} ({cols_quoted}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '\\N')",
            buffer
        )
    conn.commit()


def exportar_para_csv(df, arquivo_nome, uf, competencia):
    """Exporta o DataFrame processado para CSV."""
    from config.settings import PROJECT_ROOT
    output_dir = PROJECT_ROOT / "output" / "estabelecimentos"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Metadados
    if 'uf' not in df.columns: df['uf'] = uf
    if 'competencia' not in df.columns: df['competencia'] = competencia
    
    output_file = output_dir / f"{arquivo_nome}.csv"
    df.to_csv(output_file, index=False, sep=';', encoding='utf-8')
    print(f"   💾 Exportado: {output_file}")


def baixar_e_processar(tipo_saida="postgres"):
    """
    Função principal.
    Args:
        tipo_saida (str): 'postgres' ou 'csv'
    """
    os.makedirs(PASTA_CACHE, exist_ok=True)
    
    print("=" * 60)
    print("🏥 Carregando Histórico de Estabelecimentos (CNES)")
    print("=" * 60)
    
    conn = None
    if tipo_saida == "postgres":
        print("\n🔌 Conectando ao PostgreSQL...")
        conn = psycopg2.connect(**DB_CONFIG)
    else:
        print("\n💾 Modo CSV: Banco de dados não será acessado.")
    
    print("📡 Conectando ao FTP do DATASUS (CNES)...")
    cnes = CNES().load("ST")
    
    # Gerar lista de períodos
    periodos = []
    
    # Converter para datas para iterar
    inicio = date(ANO_INICIO, MES_INICIO, 1)
    fim = date(ANO_FIM, MES_FIM, 1)
    
    ano_atual = inicio.year
    mes_atual = inicio.month
    
    while date(ano_atual, mes_atual, 1) <= fim:
        periodos.append((ano_atual, mes_atual))
        if mes_atual == 12:
            mes_atual = 1
            ano_atual += 1
        else:
            mes_atual += 1
            
    print(f"\n📊 Processando {len(periodos)} competências para {len(UFS)} UF(s)")
    
    registros_totais = 0
    
    for uf in UFS:
        print(f"\n📍 Estado: {uf}")
        for ano, mes in tqdm(periodos, desc=f"  Periodos"):
            arquivo_nome = f"ST{uf}{str(ano)[2:]}{mes:02d}"
            parquet_path = Path(PASTA_CACHE) / f"{arquivo_nome}.parquet"
            competencia = date(ano, mes, 1)
            
            try:
                # 1. Download
                if not parquet_path.exists():
                    arquivos = cnes.get_files(group="ST", uf=uf, year=ano, month=mes)
                    if arquivos:
                        cnes.download(arquivos, local_dir=PASTA_CACHE)
                
                # 2. Leitura
                if not parquet_path.exists():
                    # Tentar achar com filtro (pysus as vezes nomeia diferente)
                    possible = list(Path(PASTA_CACHE).glob(f"{arquivo_nome}*.parquet"))
                    if possible:
                        parquet_path = possible[0]
                    else:
                        print(f"   ⚠️  Arquivo não encontrado para {uf}/{ano}/{mes}")
                        continue # Pula se não achou
                
                df = pd.read_parquet(parquet_path)
                if df.empty:
                    print(f"   ⚠️  DataFrame vazio para {uf}/{ano}/{mes}")
                    continue
                
                # 3. Processamento
                df = renomear_colunas(df)
                
                # 4. Inserção (Deleta anterior e insere novo)
                if tipo_saida == "postgres":
                    inserir_dados(conn, df, SCHEMA, TABELA, arquivo_nome, uf, competencia)
                else:
                    exportar_para_csv(df, arquivo_nome, uf, competencia)
                
                registros_totais += len(df)
                print(f"   ✅ {arquivo_nome}: {len(df):,} estabelecimentos")
                
            except Exception as e:
                print(f"   ❌ Erro em {arquivo_nome}: {e}")
                import traceback
                traceback.print_exc()
                continue
                
    if conn:
        conn.close()
    
    # Limpa cache
    if os.path.exists(PASTA_CACHE):
        shutil.rmtree(PASTA_CACHE)
        print(f"\n🧹 Cache removido: {PASTA_CACHE}/")
    
    print(f"\n{'=' * 60}")
    print(f"✅ Processamento finalizado!")
    print(f"   📊 Total de registros históricos: {registros_totais:,}")
    print(f"   🗄️  Dados em: {SCHEMA}.{TABELA} (PK: codigo_cnes, competencia)")
    print(f"   📖 Dicionário em: {SCHEMA}.dicionario_colunas")
    print(f"\n   💡 JOIN com internações:")
    print(f"      SELECT i.*, e.codigo_cnes, e.cep, e.regiao_saude")
    print(f"      FROM {SCHEMA}.internacoes i")
    print(f"      JOIN {SCHEMA}.{TABELA} e ON i.codigo_estabelecimento = e.codigo_cnes")
    print(f"      -- Para pegar o estabelecimento mais recente ou de uma competência específica:")
    print(f"      -- JOIN {SCHEMA}.{TABELA} e ON i.codigo_estabelecimento = e.codigo_cnes AND e.competencia = (SELECT MAX(competencia) FROM {SCHEMA}.{TABELA} WHERE codigo_cnes = i.codigo_estabelecimento)")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    baixar_e_processar()

