"""
Script para carregar tabela de CEPs com logradouros do CEP Aberto.
Processa os arquivos ZIP baixados e cria tabela comum.cep_logradouros.

A base do CEP Aberto contém:
- Estados: ID, Nome, Sigla
- Cidades: ID, Nome, ID_Estado
- CEPs: CEP, Logradouro, Bairro, ID_Cidade, ID_Estado

NOTA: O dump atual do CEP Aberto NÃO contém lat/long.
      Para geocodificação, usamos JOIN com a base de municípios.

Uso:
    source venv/bin/activate
    python carregar_cep_geocodificacao.py
"""

import os
import zipfile
import pandas as pd
import psycopg2
from io import StringIO
from pathlib import Path
from tqdm import tqdm

from config.settings import DB_CONFIG

SCHEMA = "comum"
TABELA_CEPS = "cep_logradouros"
TABELA_MUNICIPIOS = "municipios_coordenadas"
PASTA_ZIPS = "CEP_logra"


def extrair_csv_do_zip(arquivo_zip):
    """Extrai o CSV de dentro do arquivo ZIP."""
    with zipfile.ZipFile(arquivo_zip, 'r') as zf:
        # Pega o primeiro arquivo CSV dentro do ZIP
        for nome in zf.namelist():
            if nome.endswith('.csv') or nome.endswith('.txt') or not '.' in nome:
                with zf.open(nome) as f:
                    # Tenta diferentes encodings
                    for encoding in ['utf-8', 'latin-1', 'iso-8859-1']:
                        try:
                            f.seek(0)
                            content = f.read().decode(encoding)
                            return content
                        except:
                            continue
    return None


def carregar_estados(pasta):
    """Carrega a tabela de estados."""
    arquivo = pasta / "estados.cepaberto.zip"
    if not arquivo.exists():
        print(f"   ⚠️  Arquivo {arquivo} não encontrado")
        return pd.DataFrame()
    
    content = extrair_csv_do_zip(arquivo)
    if content:
        from io import StringIO
        df = pd.read_csv(StringIO(content), header=None, names=['id_estado', 'nome_estado', 'uf'])
        return df
    return pd.DataFrame()


def carregar_cidades(pasta):
    """Carrega a tabela de cidades."""
    arquivo = pasta / "cidades.cepaberto.zip"
    if not arquivo.exists():
        print(f"   ⚠️  Arquivo {arquivo} não encontrado")
        return pd.DataFrame()
    
    content = extrair_csv_do_zip(arquivo)
    if content:
        from io import StringIO
        df = pd.read_csv(StringIO(content), header=None, names=['id_cidade', 'nome_cidade', 'id_estado'])
        return df
    return pd.DataFrame()


def carregar_ceps_estado(arquivo_zip):
    """Carrega CEPs de um arquivo ZIP de estado."""
    content = extrair_csv_do_zip(arquivo_zip)
    if content:
        from io import StringIO
        try:
            # Formato real: CEP, Logradouro, Complemento, Bairro, ID_Cidade, ID_Estado
            df = pd.read_csv(StringIO(content), header=None, 
                           names=['cep', 'logradouro', 'complemento', 'bairro', 'id_cidade', 'id_estado'])
            return df
        except Exception as e:
            print(f"      Erro ao ler {arquivo_zip}: {e}")
    return pd.DataFrame()


def carregar_cep_logradouros():
    """Processa todos os arquivos e carrega no banco."""
    print("=" * 70)
    print("📍 Carregando CEPs com Logradouros (CEP Aberto)")
    print("=" * 70)
    
    pasta = Path(PASTA_ZIPS)
    
    if not pasta.exists():
        print(f"\n❌ Pasta {PASTA_ZIPS} não encontrada!")
        return
    
    # 1. Carrega tabelas de referência
    print("\n📂 Carregando tabelas de referência...")
    
    df_estados = carregar_estados(pasta)
    print(f"   ✅ Estados: {len(df_estados)} registros")
    
    df_cidades = carregar_cidades(pasta)
    print(f"   ✅ Cidades: {len(df_cidades):,} registros")
    
    if df_estados.empty or df_cidades.empty:
        print("\n❌ Não foi possível carregar as tabelas de referência")
        return
    
    # 2. Carrega todos os CEPs por estado
    print("\n📂 Carregando CEPs de todos os estados...")
    
    arquivos_cep = sorted(pasta.glob("*.cepaberto_parte_*.zip"))
    print(f"   📁 {len(arquivos_cep)} arquivos de CEP encontrados")
    
    todos_ceps = []
    for arquivo in tqdm(arquivos_cep, desc="   Processando"):
        df = carregar_ceps_estado(arquivo)
        if not df.empty:
            todos_ceps.append(df)
    
    if not todos_ceps:
        print("\n❌ Nenhum CEP carregado!")
        return
    
    df_ceps = pd.concat(todos_ceps, ignore_index=True)
    print(f"\n   ✅ Total de CEPs: {len(df_ceps):,}")
    
    # 3. Faz JOIN com cidades e estados
    print("\n🔗 Fazendo JOIN com cidades e estados...")
    
    # Merge com cidades
    df_ceps = df_ceps.merge(
        df_cidades[['id_cidade', 'nome_cidade']], 
        on='id_cidade', 
        how='left'
    )
    
    # Merge com estados
    df_ceps = df_ceps.merge(
        df_estados[['id_estado', 'uf']], 
        on='id_estado', 
        how='left'
    )
    
    # Formata CEP (garante 8 dígitos)
    df_ceps['cep'] = df_ceps['cep'].astype(str).str.zfill(8)
    
    # Remove duplicatas
    df_ceps = df_ceps.drop_duplicates(subset=['cep'])
    
    # Renomeia colunas
    df_ceps = df_ceps.rename(columns={'nome_cidade': 'cidade'})
    
    print(f"   ✅ CEPs únicos após JOIN: {len(df_ceps):,}")
    print(f"   🗺️  Estados: {df_ceps['uf'].nunique()}")
    print(f"   🏙️  Cidades: {df_ceps['cidade'].nunique():,}")
    
    # 4. Salva no banco
    print("\n💾 Salvando no banco...")
    conn = psycopg2.connect(**DB_CONFIG)
    
    with conn.cursor() as cur:
        # Garante schema
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
        
        # Drop e recria tabela
        cur.execute(f"DROP TABLE IF EXISTS {SCHEMA}.{TABELA_CEPS} CASCADE")
        
        cur.execute(f"""
            CREATE TABLE {SCHEMA}.{TABELA_CEPS} (
                cep CHAR(8) PRIMARY KEY,
                logradouro TEXT,
                complemento TEXT,
                bairro TEXT,
                cidade TEXT,
                uf CHAR(2),
                id_cidade INTEGER,
                id_estado INTEGER
            );
            
            COMMENT ON TABLE {SCHEMA}.{TABELA_CEPS} IS 
                'CEPs brasileiros com logradouro, bairro, cidade e UF. 
                 Fonte: CEP Aberto (cepaberto.com).
                 Para lat/long, faça JOIN com comum.municipios_coordenadas.';
            
            CREATE INDEX idx_{TABELA_CEPS}_uf ON {SCHEMA}.{TABELA_CEPS}(uf);
            CREATE INDEX idx_{TABELA_CEPS}_cidade ON {SCHEMA}.{TABELA_CEPS}(cidade);
            CREATE INDEX idx_{TABELA_CEPS}_bairro ON {SCHEMA}.{TABELA_CEPS}(bairro);
        """)
        conn.commit()
        
        # Prepara dados
        colunas = ['cep', 'logradouro', 'complemento', 'bairro', 'cidade', 'uf', 'id_cidade', 'id_estado']
        df_insert = df_ceps[colunas]
        
        buffer = StringIO()
        df_insert.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N')
        buffer.seek(0)
        
        cols_quoted = ', '.join([f'"{c}"' for c in colunas])
        cur.copy_expert(
            f"COPY {SCHEMA}.{TABELA_CEPS} ({cols_quoted}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '\\N')",
            buffer
        )
        conn.commit()
    
    # 5. Carrega base de coordenadas por município (do GitHub)
    print("\n📍 Carregando coordenadas de municípios...")
    carregar_municipios_coordenadas(conn)
    
    conn.close()
    
    print(f"\n   ✅ Tabela {SCHEMA}.{TABELA_CEPS} criada com {len(df_ceps):,} CEPs")
    
    print("\n" + "=" * 70)
    print("💡 COMO USAR:")
    print("-" * 70)
    print(f"""
-- CEPs de São Paulo com coordenadas (via município):
SELECT 
    c.cep,
    c.logradouro,
    c.bairro,
    c.cidade,
    m.latitude,
    m.longitude
FROM {SCHEMA}.{TABELA_CEPS} c
LEFT JOIN {SCHEMA}.{TABELA_MUNICIPIOS} m 
    ON c.cidade = m.municipio AND c.uf = m.uf
WHERE c.uf = 'SP'
LIMIT 100;

-- Internações com endereço e coordenadas:
SELECT 
    i.numero_aih,
    i.cep_paciente,
    c.logradouro,
    c.bairro,
    c.cidade,
    m.latitude,
    m.longitude
FROM saude.internacoes i
LEFT JOIN {SCHEMA}.{TABELA_CEPS} c ON i.cep_paciente = c.cep
LEFT JOIN {SCHEMA}.{TABELA_MUNICIPIOS} m ON c.cidade = m.municipio AND c.uf = m.uf
LIMIT 100;

-- Contagem de CEPs por estado:
SELECT uf, COUNT(*) as total_ceps 
FROM {SCHEMA}.{TABELA_CEPS} 
GROUP BY uf 
ORDER BY total_ceps DESC;
    """)
    print("=" * 70)


def carregar_municipios_coordenadas(conn):
    """Carrega base de coordenadas por município do GitHub."""
    print("   📡 Baixando base de municípios do GitHub...")
    
    try:
        url = "https://github.com/Maahzuka/database-CEPS/raw/main/ceps.xlsx"
        df = pd.read_excel(url)
        
        print(f"   📋 Colunas encontradas: {list(df.columns)[:5]}...")
        
        # Renomeia colunas (nomes reais são em MAIÚSCULAS com underscore)
        mapeamento = {
            'UF': 'uf',
            'LOCALIDADE': 'municipio',
            'LOCALIDADE_SEM_ACENTOS': 'municipio_sem_acento',
            'LATITUDE': 'latitude',
            'LONGITUDE': 'longitude',
            'COD_IBGE': 'codigo_ibge',
            'ALTITUDE': 'altitude',
            'REGIAO': 'regiao'
        }
        df = df.rename(columns=mapeamento)
        
        # Filtra colunas relevantes
        colunas = ['uf', 'municipio', 'municipio_sem_acento', 'latitude', 'longitude', 
                   'codigo_ibge', 'altitude', 'regiao']
        for col in colunas:
            if col not in df.columns:
                df[col] = None
        
        # Converte coordenadas de formato brasileiro (vírgula) para float
        def parse_coord(val):
            if pd.isna(val):
                return None
            if isinstance(val, str):
                return float(val.replace(',', '.'))
            return float(val)
        
        df['latitude'] = df['latitude'].apply(parse_coord)
        df['longitude'] = df['longitude'].apply(parse_coord)
        df['altitude'] = df['altitude'].apply(parse_coord)
        
        df = df[colunas].dropna(subset=['latitude', 'longitude'])
        df = df.drop_duplicates(subset=['municipio', 'uf'])
        
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {SCHEMA}.{TABELA_MUNICIPIOS} CASCADE")
            
            cur.execute(f"""
                CREATE TABLE {SCHEMA}.{TABELA_MUNICIPIOS} (
                    id SERIAL PRIMARY KEY,
                    uf CHAR(2),
                    municipio TEXT,
                    municipio_sem_acento TEXT,
                    latitude DOUBLE PRECISION,
                    longitude DOUBLE PRECISION,
                    codigo_ibge TEXT,
                    altitude DOUBLE PRECISION,
                    regiao TEXT,
                    UNIQUE(municipio, uf)
                );
                
                COMMENT ON TABLE {SCHEMA}.{TABELA_MUNICIPIOS} IS 
                    'Coordenadas (centróide) de municípios brasileiros. 
                     Use para JOIN com cep_logradouros.
                     Fonte: Correios/IBGE via GitHub.';
                
                CREATE INDEX idx_{TABELA_MUNICIPIOS}_uf ON {SCHEMA}.{TABELA_MUNICIPIOS}(uf);
                CREATE INDEX idx_{TABELA_MUNICIPIOS}_nome ON {SCHEMA}.{TABELA_MUNICIPIOS}(municipio);
            """)
            conn.commit()
            
            buffer = StringIO()
            df.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N')
            buffer.seek(0)
            
            cols_quoted = ', '.join([f'"{c}"' for c in colunas])
            cur.copy_expert(
                f"COPY {SCHEMA}.{TABELA_MUNICIPIOS} ({cols_quoted}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '\\N')",
                buffer
            )
            conn.commit()
        
        print(f"   ✅ Tabela {SCHEMA}.{TABELA_MUNICIPIOS} criada com {len(df):,} municípios")
        
    except Exception as e:
        print(f"   ⚠️  Erro ao carregar municípios: {e}")


if __name__ == "__main__":
    carregar_cep_logradouros()
