"""
Script para baixar dados de internações do SIH-SUS (DATASUS) e inserir no PostgreSQL.

Características:
- Nomes intuitivos: colunas renomeadas para melhor legibilidade
- Referência DATASUS: nome original guardado no dicionário e COMMENT
- Schema dinâmico: cria/atualiza tabela baseado nas colunas do Parquet
- UF automático: infere do nome do arquivo (RDSP2301 -> SP)
- Proteção anti-duplicação: verifica antes de inserir

Uso:
    source venv/bin/activate
    python -m etl.saude.internacoes
"""

import os
import shutil
import time
import socket
from pathlib import Path
from pysus.ftp.databases.sih import SIH
import pandas as pd
import psycopg2
from tqdm import tqdm
import sys
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Adiciona raiz do projeto ao path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dicionarios.sihsus import (
    obter_nome_intuitivo, 
    obter_descricao, 
    obter_mapeamento_colunas,
    obter_dicionario_completo
)

# --- CONFIGURAÇÕES ---
# Conexão PostgreSQL e Configuração Modular
from config.settings import DB_CONFIG, ETL_CONFIG

# Obtém configs específicas ou usa defaults
CONF = ETL_CONFIG.get("internacoes", {})
UFS = CONF.get("ufs", ETL_CONFIG["padrao"]["ufs"])
ANO_INICIO = CONF.get("ano_inicio", 2015)
ANO_FIM = CONF.get("ano_fim", 2025)

PASTA_CACHE = "cache/internacoes" # Ajustado para pasta mais organizada
SCHEMA = "saude"
TABELA = "internacoes"
LIMPAR_CACHE_AO_FINALIZAR = True

# Configurações de retry para download
RETRY_MAX_TENTATIVAS = 5
RETRY_ESPERA_INICIAL = 5  # segundos
RETRY_ESPERA_MAXIMA = 120  # segundos


def mapear_tipo_postgres(dtype):
    """Mapeia tipos do pandas/pyarrow para tipos do PostgreSQL."""
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


def renomear_colunas_df(df):
    """Renomeia as colunas do DataFrame para nomes intuitivos."""
    mapeamento = obter_mapeamento_colunas()
    novas_colunas = {}
    
    for col in df.columns:
        col_lower = col.lower()
        if col_lower in mapeamento:
            novas_colunas[col] = mapeamento[col_lower]
        else:
            novas_colunas[col] = col_lower
    
    return df.rename(columns=novas_colunas)


def obter_colunas_tabela(conn, schema, tabela):
    """Retorna set de colunas existentes na tabela."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = %s AND table_name = %s
        """, (schema, tabela))
        return {row[0].lower() for row in cur.fetchall()}


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


def criar_tabela_dicionario(conn, schema):
    """Cria a tabela de dicionário de colunas com referência ao DATASUS."""
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
            
            COMMENT ON TABLE {schema}.dicionario_colunas IS 
                'Dicionário de dados com descrição de todas as colunas. Inclui referência ao nome original do DATASUS.';
        """)
        
        # Insere dados do dicionário
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
                'internacoes',
                nome_intuitivo,
                nome_original,
                info.get('descricao', ''),
                info.get('tipo', ''),
                info.get('exemplo', '')
            ))
        
        conn.commit()
        print(f"   ✅ Tabela {schema}.dicionario_colunas criada com {len(dicionario)} entradas")


def adicionar_comments_colunas(conn, schema, tabela):
    """Adiciona COMMENT em cada coluna (aparece no DBeaver!)."""
    colunas = obter_colunas_tabela(conn, schema, tabela)
    dicionario = obter_dicionario_completo()
    
    # Criar mapeamento inverso: nome_intuitivo -> nome_original
    mapeamento_inverso = {}
    for nome_orig, info in dicionario.items():
        nome_int = info.get('nome_intuitivo', nome_orig)
        mapeamento_inverso[nome_int] = nome_orig
    
    with conn.cursor() as cur:
        for coluna in colunas:
            nome_original = mapeamento_inverso.get(coluna, coluna)
            if nome_original in dicionario:
                info = dicionario[nome_original]
                descricao = f"{info.get('descricao', '')} [DATASUS: {nome_original}]"
            else:
                descricao = f"Coluna do SIHSUS"
            
            descricao_escaped = descricao.replace("'", "''")
            try:
                cur.execute(f"""
                    COMMENT ON COLUMN {schema}.{tabela}."{coluna}" IS '{descricao_escaped}'
                """)
            except Exception:
                pass
        
        conn.commit()


def criar_ou_atualizar_tabela(conn, df, schema, tabela):
    """Cria a tabela PARTICIONADA se não existir ou adiciona novas colunas se necessário."""
    # O DataFrame já vem com colunas renomeadas
    
    with conn.cursor() as cur:
        if not tabela_existe(conn, schema, tabela):
            # Cria a tabela PARTICIONADA por UF e ANO
            colunas_sql = ['id SERIAL']  # Sem PRIMARY KEY em tabela particionada
            colunas_sql.append('"uf" CHAR(2) NOT NULL')
            colunas_sql.append('"ano" INTEGER NOT NULL')  # Coluna de partição
            colunas_sql.append('"arquivo_origem" TEXT NOT NULL')
            colunas_sql.append('"data_carga" TIMESTAMP DEFAULT CURRENT_TIMESTAMP')
            
            for col in df.columns:
                col_lower = col.lower()
                if col_lower not in ['uf', 'arquivo_origem', 'ano']:
                    tipo = mapear_tipo_postgres(df[col].dtype)
                    colunas_sql.append(f'"{col_lower}" {tipo}')
            
            # Tabela principal particionada por UF e ANO (LIST + RANGE usando composta)
            # Usamos PARTITION BY LIST para uma chave composta "uf_ano"
            create_sql = f'''
                CREATE TABLE {schema}.{tabela} (
                    {", ".join(colunas_sql)},
                    PRIMARY KEY (id, uf, ano)
                ) PARTITION BY LIST (uf)
            '''
            cur.execute(create_sql)
            
            # COMMENT na tabela
            cur.execute(f"""
                COMMENT ON TABLE {schema}.{tabela} IS 
                    'Dados de internações hospitalares do SIH-SUS. PARTICIONADA por UF e ANO. Colunas renomeadas para nomes intuitivos.'
            """)
            
            conn.commit()
            print(f"   ✅ Tabela {schema}.{tabela} criada (PARTICIONADA por UF → ANO)")
            
            # COMMENT nas colunas
            adicionar_comments_colunas(conn, schema, tabela)
            print(f"   ✅ Comentários adicionados (com referência DATASUS)")
            
            # Tabela dicionário
            criar_tabela_dicionario(conn, schema)
            
        else:
            # Verifica novas colunas (na tabela principal, reflete nas partições)
            colunas_existentes = obter_colunas_tabela(conn, schema, tabela)
            
            for col in df.columns:
                col_lower = col.lower()
                if col_lower not in colunas_existentes and col_lower not in ['ano']:
                    tipo = mapear_tipo_postgres(df[col].dtype)
                    cur.execute(f'ALTER TABLE {schema}.{tabela} ADD COLUMN IF NOT EXISTS "{col_lower}" {tipo}')
            
            conn.commit()


def criar_particao_se_necessario(conn, schema, tabela, uf, ano):
    """Cria partição para UF/ANO se não existir."""
    nome_particao_uf = f"{tabela}_{uf.lower()}"
    nome_particao_ano = f"{tabela}_{uf.lower()}_{ano}"
    
    with conn.cursor() as cur:
        # Verifica se partição da UF existe
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM pg_tables 
                WHERE schemaname = %s AND tablename = %s
            )
        """, (schema, nome_particao_uf))
        uf_existe = cur.fetchone()[0]
        
        if not uf_existe:
            # Cria partição da UF (sub-particionada por ANO)
            cur.execute(f"""
                CREATE TABLE {schema}.{nome_particao_uf} 
                PARTITION OF {schema}.{tabela}
                FOR VALUES IN ('{uf}')
                PARTITION BY RANGE (ano)
            """)
            conn.commit()
        
        # Verifica se partição do ANO existe
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM pg_tables 
                WHERE schemaname = %s AND tablename = %s
            )
        """, (schema, nome_particao_ano))
        ano_existe = cur.fetchone()[0]
        
        if not ano_existe:
            # Cria partição do ANO
            cur.execute(f"""
                CREATE TABLE {schema}.{nome_particao_ano}
                PARTITION OF {schema}.{nome_particao_uf}
                FOR VALUES FROM ({ano}) TO ({ano + 1})
            """)
            # Cria índices na partição
            cur.execute(f'CREATE INDEX IF NOT EXISTS idx_{nome_particao_ano}_data ON {schema}.{nome_particao_ano}(data_internacao)')
            cur.execute(f'CREATE INDEX IF NOT EXISTS idx_{nome_particao_ano}_diag ON {schema}.{nome_particao_ano}(diagnostico_principal)')
            cur.execute(f'CREATE INDEX IF NOT EXISTS idx_{nome_particao_ano}_arquivo ON {schema}.{nome_particao_ano}(arquivo_origem)')
            conn.commit()


def arquivo_ja_carregado(conn, schema, tabela, arquivo):
    """Verifica se o arquivo já foi carregado no banco."""
    if not tabela_existe(conn, schema, tabela):
        return False
        
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT COUNT(*) FROM {schema}.{tabela} 
            WHERE arquivo_origem = %s 
            LIMIT 1
        """, (arquivo,))
        return cur.fetchone()[0] > 0


def inserir_dados(conn, df, schema, tabela, arquivo, uf):
    """Insere os dados do DataFrame no PostgreSQL (tabela particionada)."""
    df = df.copy()
    df['arquivo_origem'] = arquivo
    df['uf'] = uf
    
    # Extrai o ano do nome do arquivo (ex: RDSP2301 -> 2023)
    ano_curto = arquivo[4:6]  # "23" de "RDSP2301"
    ano = 2000 + int(ano_curto) if int(ano_curto) < 50 else 1900 + int(ano_curto)
    df['ano'] = ano
    
    # Cria partição para UF/ANO se não existir
    criar_particao_se_necessario(conn, schema, tabela, uf, ano)
    
    # Já está com nomes intuitivos, só normaliza para lowercase
    df.columns = [c.lower() for c in df.columns]
    
    colunas_tabela = obter_colunas_tabela(conn, schema, tabela)
    colunas_tabela = colunas_tabela - {'id', 'data_carga'}
    
    colunas_insert = [c for c in df.columns if c in colunas_tabela]
    df_insert = df[colunas_insert]
    
    from io import StringIO
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


def exportar_para_csv(df, arquivo_nome, uf):
    """Exporta o DataFrame processado para CSV."""
    from config.settings import PROJECT_ROOT
    output_dir = PROJECT_ROOT / "output" / "internacoes"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Adicionar colunas de metadados se não existirem
    if 'uf' not in df.columns: df['uf'] = uf
    if 'arquivo_origem' not in df.columns: df['arquivo_origem'] = arquivo_nome
    
    output_file = output_dir / f"{arquivo_nome}.csv"
    df.to_csv(output_file, index=False, sep=';', encoding='utf-8')
    print(f"   💾 Exportado: {output_file}")


def extrair_uf_do_arquivo(nome_arquivo):
    """Extrai a UF do nome do arquivo (RDSP2301 -> SP)."""
    if len(nome_arquivo) >= 4:
        return nome_arquivo[2:4].upper()
    return None


# Decorador de retry para downloads do FTP
def _download_com_retry(sih, arquivos, pasta_cache, max_tentativas=RETRY_MAX_TENTATIVAS):
    """
    Tenta fazer download com retry e backoff exponencial.
    
    Args:
        sih: Instância do SIH
        arquivos: Lista de arquivos para download
        pasta_cache: Diretório destino
        max_tentativas: Número máximo de tentativas
        
    Returns:
        Resultado do download ou None se falhar
    """
    tentativa = 0
    ultimo_erro = None
    
    while tentativa < max_tentativas:
        tentativa += 1
        try:
            result = sih.download(arquivos, local_dir=pasta_cache)
            return result
            
        except (socket.gaierror, socket.timeout, ConnectionError, OSError) as e:
            ultimo_erro = e
            if tentativa < max_tentativas:
                # Backoff exponencial: 5s, 10s, 20s, 40s, 80s
                espera = min(RETRY_ESPERA_INICIAL * (2 ** (tentativa - 1)), RETRY_ESPERA_MAXIMA)
                tqdm.write(f"   ⚠️  Tentativa {tentativa}/{max_tentativas} falhou: {type(e).__name__}")
                tqdm.write(f"   ⏳ Aguardando {espera}s antes de tentar novamente...")
                time.sleep(espera)
            else:
                tqdm.write(f"   ❌ Todas as {max_tentativas} tentativas falharam.")
                raise ultimo_erro
                
        except Exception as e:
            # Outros erros não são retryable
            raise e
    
    return None


def baixar_e_processar(tipo_saida="postgres"):
    """
    Função principal.
    Args:
        tipo_saida (str): 'postgres' ou 'csv'
    """
    os.makedirs(PASTA_CACHE, exist_ok=True)
    
    conn = None
    if tipo_saida == "postgres":
        print("🔌 Conectando ao PostgreSQL...")
        conn = psycopg2.connect(**DB_CONFIG)
    else:
        print("💾 Modo CSV: Banco de dados não será acessado.")
    
    print("📡 Conectando ao FTP do DATASUS...")
    sih = SIH().load()
    
    periodos = []
    for ano in range(ANO_INICIO, ANO_FIM + 1):
        for mes in range(1, 13):
            periodos.append((ano, mes))
    
    total_arquivos = len(UFS) * len(periodos)
    print(f"\n📊 Processando {len(UFS)} UF(s) x {len(periodos)} meses = {total_arquivos} arquivos\n")
    
    arquivos_processados = 0
    arquivos_pulados = 0
    registros_totais = 0
    
    for uf in UFS:
        print(f"\n🏥 Estado: {uf}")
        
        # Processa ANO a ANO (permite limpeza de memória entre anos)
        for ano in range(ANO_INICIO, ANO_FIM + 1):
            ano_registros = 0
            
            for mes in tqdm(range(1, 13), desc=f"  {uf} {ano}", leave=True):
                arquivo_nome = f"RD{uf}{str(ano)[2:]}{mes:02d}"
                parquet_path = Path(PASTA_CACHE) / f"{arquivo_nome}.parquet"
                
                try:
                    # Se for postgres, checa se já existe
                    if tipo_saida == "postgres":
                        if arquivo_ja_carregado(conn, SCHEMA, TABELA, arquivo_nome):
                            arquivos_pulados += 1
                            continue
                    else:
                        # Se for CSV, verifica se arquivo CSV já existe
                        from config.settings import PROJECT_ROOT
                        csv_path = PROJECT_ROOT / "output" / "internacoes" / f"{arquivo_nome}.csv"
                        if csv_path.exists():
                            continue
                    
                    if not parquet_path.exists():
                        arquivos = sih.get_files(group="RD", uf=uf, year=ano, month=mes)
                        
                        if not arquivos:
                            continue
                        
                        # Download com retry automático
                        result = _download_com_retry(sih, arquivos, PASTA_CACHE)
                        
                        if result is None:
                            continue
                    
                    if parquet_path.exists():
                        df = pd.read_parquet(parquet_path)
                    else:
                        possible_files = list(Path(PASTA_CACHE).glob(f"{arquivo_nome}*.parquet"))
                        if not possible_files:
                            continue
                        df = pd.read_parquet(possible_files[0])
                        parquet_path = possible_files[0]
                    
                    if df.empty:
                        continue
                    
                    # RENOMEIA COLUNAS PARA NOMES INTUITIVOS
                    df = renomear_colunas_df(df)
                    
                    if tipo_saida == "postgres":
                        criar_ou_atualizar_tabela(conn, df, SCHEMA, TABELA)
                        inserir_dados(conn, df, SCHEMA, TABELA, arquivo_nome, uf)
                    else:
                        exportar_para_csv(df, arquivo_nome, uf)
                    
                    arquivos_processados += 1
                    ano_registros += len(df)
                    registros_totais += len(df)
                    
                    # Libera memória do DataFrame após uso
                    del df
                    
                except Exception as e:
                    print(f"❌ Erro ao processar {arquivo_nome}: {e}")
                    import traceback
                    traceback.print_exc()
                    continue
            
            # === LIMPEZA APÓS CADA ANO ===
            if ano_registros > 0:
                tqdm.write(f"   🗓️  {uf} {ano}: {ano_registros:,} registros")
            
            # 1. Limpa cache dos arquivos do ano
            for f in Path(PASTA_CACHE).glob(f"RD{uf}{str(ano)[2:]}*.parquet"):
                try:
                    f.unlink()
                except Exception:
                    pass
            for f in Path(PASTA_CACHE).glob(f"RD{uf}{str(ano)[2:]}*.dbc"):
                try:
                    f.unlink()
                except Exception:
                    pass
            
            # 2. Força garbage collection para liberar memória
            import gc
            gc.collect()
            
            # 3. Commit no banco para persistir dados
            if conn:
                conn.commit()
    
    if conn:
        conn.close()
    
    # Limpa todo cache restante ao final
    if LIMPAR_CACHE_AO_FINALIZAR and os.path.exists(PASTA_CACHE):
        shutil.rmtree(PASTA_CACHE)
        print(f"\n🧹 Cache removido: {PASTA_CACHE}/")
    
    print(f"\n{'='*60}")
    print(f"✅ Processamento finalizado!")
    print(f"   📁 Arquivos novos: {arquivos_processados}")
    print(f"   ⏭️  Arquivos pulados: {arquivos_pulados}")
    print(f"   📊 Registros inseridos: {registros_totais:,}")
    print(f"   🗄️  Dados em: {SCHEMA}.{TABELA}")
    print(f"   📖 Dicionário em: {SCHEMA}.dicionario_colunas")
    print(f"{'='*60}")


if __name__ == "__main__":
    baixar_e_processar()
