#!/usr/bin/env python3
"""
ETL - Carregar dados de renda do Censo por Setor Censitário

Fontes:
- Censo 2010: Agregados por Setores Censitários (IBGE FTP)
- Censo 2022: Microdados (quando disponível por setor)

Uso:
    python -m etl.comum.setor_renda

Saída:
    Tabela: comum.setor_renda (~90K setores SP, ~30MB)
"""

import os
import sys
import json
import zipfile
import requests
import numpy as np
import pandas as pd
import psycopg2
from pathlib import Path
from io import StringIO
from tqdm import tqdm

# Adiciona raiz do projeto ao path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# ============================================================
# CONFIGURAÇÃO
# ============================================================

from config.settings import DB_CONFIG, ETL_CONFIG

# Obtém configs específicas ou usa defaults
CONF = ETL_CONFIG.get("censo_renda", {})
UFS = CONF.get("ufs", ETL_CONFIG["padrao"]["ufs"])

SCHEMA = "comum"
TABELA = "setor_renda"
PASTA_CACHE = Path("cache/censo_renda")

# URLs dos dados do Censo 2010 (Agregados por Setores)
BASE_URL_CENSO_2010 = "https://ftp.ibge.gov.br/Censos/Censo_Demografico_2010/Resultados_do_Universo/Agregados_por_Setores_Censitarios"

# Mapeamento UF -> código
UF_CODIGOS = {
    "AC": "12", "AL": "27", "AM": "13", "AP": "16", "BA": "29",
    "CE": "23", "DF": "53", "ES": "32", "GO": "52", "MA": "21",
    "MG": "31", "MS": "50", "MT": "51", "PA": "15", "PB": "25",
    "PE": "26", "PI": "22", "PR": "41", "RJ": "33", "RN": "24",
    "RO": "11", "RR": "14", "RS": "43", "SC": "42", "SE": "28",
    "SP": "35", "TO": "17"
}

# URL Censo 2022 (Renda Responsável)
URL_CENSO_2022_RENDA = "ftp://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/Agregados_por_Setores_Censitarios_Rendimento_do_Responsavel/Agregados_por_setores_renda_responsavel_BR_csv.zip"


# ============================================================
# FUNÇÕES AUXILIARES
# ============================================================

def carregar_config_classes():
    """Carrega configuração de classes de renda do JSON."""
    config_path = Path(__file__).parent.parent.parent / "config" / "classes_renda.json"
    
    if not config_path.exists():
        print(f"   ⚠️  Arquivo {config_path} não encontrado. Usando padrão.")
        return {
            "salario_minimo": 1412.00,
            "classes": [
                {"classe": "E", "nome": "Extrema pobreza", "limite_superior_sm": 0.25},
                {"classe": "D", "nome": "Baixa renda", "limite_superior_sm": 0.5},
                {"classe": "C", "nome": "Classe média baixa", "limite_superior_sm": 1.0},
                {"classe": "B", "nome": "Classe média", "limite_superior_sm": 2.0},
                {"classe": "A", "nome": "Alta renda", "limite_superior_sm": None}
            ]
        }
    
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def calcular_classe(renda_per_capita, config, salario_minimo):
    """Calcula classe de renda baseada na configuração e salário mínimo específico."""
    if pd.isna(renda_per_capita) or renda_per_capita <= 0:
        return None, None
    
    # Usar salario minimo passado como argumento
    sm = salario_minimo
    
    for c in config['classes']:
        limite = c.get('limite_superior_sm')
        if limite is None:
            return c['classe'], c['nome']
        if renda_per_capita <= sm * limite:
            return c['classe'], c['nome']
    
    # Fallback para última classe
    ultima = config['classes'][-1]
    return ultima['classe'], ultima['nome']


def calcular_quartil_decil(df, coluna='renda_per_capita'):
    """Calcula quartis e decis para uma coluna."""
    df['renda_quartil'] = pd.qcut(
        df[coluna].rank(method='first'), 
        q=4, 
        labels=[1, 2, 3, 4]
    ).astype(int)
    
    df['renda_decil'] = pd.qcut(
        df[coluna].rank(method='first'), 
        q=10, 
        labels=list(range(1, 11))
    ).astype(int)
    
    return df


# ============================================================
# TABELA
# ============================================================

def criar_tabela(conn):
    """Cria a tabela de renda por setor censitário."""
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
        cur.execute(f"DROP TABLE IF EXISTS {SCHEMA}.{TABELA} CASCADE")
        
        cur.execute(f"""
            CREATE TABLE {SCHEMA}.{TABELA} (
                id SERIAL PRIMARY KEY,
                codigo_setor VARCHAR(20),
                uf CHAR(2),
                codigo_municipio VARCHAR(7),
                municipio TEXT,
                
                -- Valores originais
                renda_media_domicilio DECIMAL(12,2),
                renda_per_capita DECIMAL(12,2),          -- V005 (2010) / NULL (2022)
                renda_responsavel_com_renda DECIMAL(12,2), -- V007 (2010) / V06004 (2022)
                log_renda_per_capita DECIMAL(8,4),
                
                -- Classes (Dupla classificação)
                classe_renda_per_capita CHAR(1),         -- Baseado em V005 (Vulnerabilidade real)
                classe_renda_per_capita_nome TEXT,
                
                classe_renda_responsavel CHAR(1),        -- Baseado em V007/V06004 (Poder de compra ocupados)
                classe_renda_responsavel_nome TEXT,
                
                -- Quartis (baseado em renda_per_capita quando existe, ou renda_responsavel)
                renda_quartil INTEGER,
                renda_decil INTEGER,
                
                -- Metadados
                salario_minimo_ref DECIMAL(10,2),
                data_ref DATE,
                fonte TEXT,
                
                UNIQUE(codigo_setor, fonte)
            );
            
            CREATE INDEX idx_{TABELA}_setor ON {SCHEMA}.{TABELA}(codigo_setor);
            CREATE INDEX idx_{TABELA}_uf ON {SCHEMA}.{TABELA}(uf);
            CREATE INDEX idx_{TABELA}_classe_pc ON {SCHEMA}.{TABELA}(classe_renda_per_capita);
            CREATE INDEX idx_{TABELA}_classe_resp ON {SCHEMA}.{TABELA}(classe_renda_responsavel);
            CREATE INDEX idx_{TABELA}_fonte ON {SCHEMA}.{TABELA}(fonte);
            
            -- Comentário da tabela
            COMMENT ON TABLE {SCHEMA}.{TABELA} IS 
                'Renda por setor censitário (Censo IBGE). Liga com comum.cep_geocodificado via codigo_setor.';
            
            -- Comentários das colunas
            COMMENT ON COLUMN {SCHEMA}.{TABELA}.renda_per_capita IS 
                'CENSO 2010 (V005): Renda média de TODOS os responsáveis (inclui sem renda). Indicador "padrão ouro" de vulnerabilidade. NULL em 2022.';
            COMMENT ON COLUMN {SCHEMA}.{TABELA}.renda_responsavel_com_renda IS 
                'CENSO 2010 (V007) / 2022 (V06004): Renda média apenas dos responsáveis COM RENDIMENTO. Superestima a riqueza da região. Útil para comparar poder de compra de quem trabalha.';
            COMMENT ON COLUMN {SCHEMA}.{TABELA}.classe_renda_per_capita IS 
                'Classificação A-E baseada na renda per capita (V005). Disponível apenas para 2010 por enquanto.';
            COMMENT ON COLUMN {SCHEMA}.{TABELA}.classe_renda_responsavel IS 
                'Classificação A-E baseada na renda dos responsáveis com rendimento. Disponível para 2010 e 2022. CUIDADO: Tende a classificar como mais rico do que a realidade.';
        """)
        conn.commit()
        print(f"   ✅ Tabela {SCHEMA}.{TABELA} criada com comentários")


# ============================================================
# DOWNLOAD CENSO 2010
# ============================================================

def baixar_censo_2010(uf):
    """Baixa dados do Censo 2010 para uma UF."""
    os.makedirs(PASTA_CACHE, exist_ok=True)
    
    arquivos_baixados = []
    
    # SP é dividido em Capital e Exceto_Capital
    if uf == "SP":
        arquivos_uf = [
            ("SP_Capital_20231030.zip", "SP_Capital"),
            ("SP_Exceto_Capital_20231030.zip", "SP_Exceto_Capital")
        ]
    else:
        arquivos_uf = [(f"{uf}_20231030.zip", uf)]
    
    for nome_arquivo, label in arquivos_uf:
        arquivo_zip = PASTA_CACHE / nome_arquivo
        
        if arquivo_zip.exists():
            print(f"   📂 Usando cache: {nome_arquivo}")
            arquivos_baixados.append(arquivo_zip)
            continue
        
        # Constrói URL do arquivo
        url = f"{BASE_URL_CENSO_2010}/{nome_arquivo}"
        
        # URL via HTTPS (mais seguro e fácil com requests)
        url_https = url
        
        print(f"   📥 Baixando {label}...")
        try:
            # Substituindo curl por requests com stream
            with requests.get(url_https, stream=True, timeout=600) as r:
                r.raise_for_status()
                total_size = int(r.headers.get('content-length', 0))
                
                with open(arquivo_zip, 'wb') as f, tqdm(
                    desc=nome_arquivo,
                    total=total_size,
                    unit='iB',
                    unit_scale=True,
                    unit_divisor=1024,
                ) as bar:
                    for chunk in r.iter_content(chunk_size=8192):
                        size = f.write(chunk)
                        bar.update(size)
            
            if arquivo_zip.exists() and arquivo_zip.stat().st_size > 1000:
                print(f"   ✅ {label} baixado ({arquivo_zip.stat().st_size / 1024 / 1024:.1f} MB)")
                arquivos_baixados.append(arquivo_zip)
            else:
                print(f"   ❌ Erro ao baixar {label} (Tamanho inválido)")
                if arquivo_zip.exists(): os.remove(arquivo_zip)
                
        except Exception as e:
            print(f"   ❌ Erro ao baixar {label}: {e}")
            if arquivo_zip.exists(): os.remove(arquivo_zip)
    
    return arquivos_baixados if arquivos_baixados else None
    
    return arquivos_baixados if arquivos_baixados else None


def baixar_censo_2022(uf):
    """Baixa dados de renda do Censo 2022 (BR)."""
    os.makedirs(PASTA_CACHE / "2022", exist_ok=True)
    
    # O arquivo é BR (Brasil todo), então baixamos apenas uma vez
    arquivo_zip = PASTA_CACHE / "2022" / "Agregados_por_setores_renda_responsavel_BR_csv.zip"
    
    if arquivo_zip.exists():
        print(f"   📂 Usando cache 2022: {arquivo_zip.name}")
        return arquivo_zip
        
    print(f"   📥 Baixando Censo 2022 (Brasil)...")
    try:
        # Usar requests com HTTPS
        url_https = URL_CENSO_2022_RENDA.replace("ftp://", "https://")
        
        with requests.get(url_https, stream=True, timeout=1200) as r:
            r.raise_for_status()
            total_size = int(r.headers.get('content-length', 0))
            
            with open(arquivo_zip, 'wb') as f, tqdm(
                desc="Censo 2022",
                total=total_size,
                unit='iB',
                unit_scale=True,
                unit_divisor=1024,
            ) as bar:
                for chunk in r.iter_content(chunk_size=8192):
                    size = f.write(chunk)
                    bar.update(size)
        
        if arquivo_zip.exists() and arquivo_zip.stat().st_size > 1000:
            print(f"   ✅ Censo 2022 baixado ({arquivo_zip.stat().st_size / 1024 / 1024:.1f} MB)")
            return arquivo_zip
        else:
            print(f"   ❌ Erro ao baixar Censo 2022")
            if arquivo_zip.exists(): os.remove(arquivo_zip)
            return None
            
    except Exception as e:
        print(f"   ❌ Erro ao baixar Censo 2022: {e}")
        return None


def processar_censo_2010(arquivo_zip, uf, config):
    """Processa arquivo ZIP do Censo 2010 e extrai dados de renda."""
    
    if not arquivo_zip or not arquivo_zip.exists():
        return None
    
    print(f"   📊 Processando {uf}...")
    
    try:
        with zipfile.ZipFile(arquivo_zip, 'r') as z:
            arquivos = z.namelist()
            
            # Usar arquivo Basico que tem população e renda corretamente
            arquivo_basico = None
            for arq in arquivos:
                if 'Basico' in arq and arq.endswith('.csv'):
                    arquivo_basico = arq
                    break
            
            if not arquivo_basico:
                print(f"   ⚠️  Arquivo Basico não encontrado em {arquivo_zip.name}")
                return None
            
            print(f"   📄 Lendo: {arquivo_basico}")
            
            with z.open(arquivo_basico) as f:
                df = pd.read_csv(f, sep=';', encoding='latin-1', low_memory=False,
                                decimal=',')  # CSV usa vírgula como decimal
            
            print(f"   📋 Colunas: {list(df.columns)[:15]}...")
            
            # Colunas do Basico:
            # Cod_setor, V001 (domicílios), V002 (população)
            # V006 (renda nominal mensal total), V008 (renda com ajuste)
            
            col_setor = 'Cod_setor'
            if col_setor not in df.columns:
                colunas_setor = [c for c in df.columns if 'setor' in c.lower()]
                col_setor = colunas_setor[0] if colunas_setor else df.columns[0]
            
            resultado = pd.DataFrame()
            resultado['codigo_setor'] = df[col_setor].astype(str)
            resultado['uf'] = uf
            
            # Extrair código do município (Cod_municipio no Basico)
            col_cod_mun = None
            for c in df.columns:
                if c == 'Cod_municipio' or 'cod_municipio' in c.lower():
                    col_cod_mun = c
                    break
            if col_cod_mun:
                resultado['codigo_municipio'] = df[col_cod_mun].astype(str)
            
            # Extrair nome do município (Nome_do_municipio no Basico)
            col_nome_mun = None
            for c in df.columns:
                if 'nome' in c.lower() and 'municipio' in c.lower():
                    col_nome_mun = c
                    break
            if col_nome_mun:
                resultado['municipio'] = df[col_nome_mun]
            
            # Variáveis de renda do Basico (documentação IBGE):
            # V005 = Rendimento nominal médio mensal dos responsáveis (COM e SEM rendimento)
            # V006 = Variância de V005
            # V007 = Rendimento nominal médio mensal dos responsáveis (APENAS COM rendimento)
            # V008 = Variância de V007
            # 
            # Usamos V005 para capturar vulnerabilidade social (inclui desempregados)
            
            if 'V005' in df.columns:
                resultado['renda_per_capita'] = pd.to_numeric(df['V005'], errors='coerce')
            elif 'V007' in df.columns:
                # Fallback para V007 se V005 não existir (mas idealmente V005 é prioridade)
                resultado['renda_per_capita'] = pd.to_numeric(df['V007'], errors='coerce')
            else:
                print("   ⚠️  Colunas V005/V007 não encontradas")
                return None
            
            # V007: Renda dos responsáveis COM rendimento (para comparação com 2022)
            if 'V007' in df.columns:
                resultado['renda_responsavel_com_renda'] = pd.to_numeric(df['V007'], errors='coerce')
            else:
                resultado['renda_responsavel_com_renda'] = None

            # Renda média por domicílio = renda_per_capita × média de moradores
            if 'V003' in df.columns:
                resultado['renda_media_domicilio'] = resultado['renda_per_capita'] * pd.to_numeric(df['V003'], errors='coerce')
            else:
                resultado['renda_media_domicilio'] = resultado['renda_per_capita'] * 3
            
            # Log renda (baseado na per capita)
            resultado['log_renda_per_capita'] = np.log(resultado['renda_per_capita'].clip(lower=1))
            
            # Definir salário mínimo de 2010
            sm_2010 = config['salarios_minimos']['2010']
            
            # Calcular classes (DUPLA CLASSIFICAÇÃO)
            resultado['classe_renda_per_capita'] = None
            resultado['classe_renda_per_capita_nome'] = None
            resultado['classe_renda_responsavel'] = None
            resultado['classe_renda_responsavel_nome'] = None
            
            for idx, row in resultado.iterrows():
                # Classe 1: Per Capita (Vulnerabilidade Real)
                cl_pc, nome_pc = calcular_classe(row['renda_per_capita'], config, sm_2010)
                resultado.at[idx, 'classe_renda_per_capita'] = cl_pc
                resultado.at[idx, 'classe_renda_per_capita_nome'] = nome_pc
                
                # Classe 2: Responsável com Renda (Poder de Compra)
                cl_resp, nome_resp = calcular_classe(row.get('renda_responsavel_com_renda'), config, sm_2010)
                resultado.at[idx, 'classe_renda_responsavel'] = cl_resp
                resultado.at[idx, 'classe_renda_responsavel_nome'] = nome_resp
            
            # Remover linhas sem renda válida
            resultado = resultado.dropna(subset=['renda_per_capita'])
            resultado = resultado[resultado['renda_per_capita'] > 0]
            
            # Calcular quartis e decis (baseado na per capita, que é o principal indicador de 2010)
            if len(resultado) > 10:
                resultado = calcular_quartil_decil(resultado, coluna='renda_per_capita')
            
            # Metadados
            resultado['salario_minimo_ref'] = sm_2010
            resultado['data_ref'] = '2010-08-01'
            resultado['fonte'] = 'CENSO_2010'
            
            print(f"   ✅ {len(resultado):,} setores processados")
            return resultado
            
    except Exception as e:
        print(f"   ❌ Erro ao processar: {e}")
        return None


def processar_censo_2022(arquivo_zip, uf, config):
    """Processa Censo 2022 (apenas renda responsável com rendimento)."""
    if not arquivo_zip or not arquivo_zip.exists():
        return None
        
    print(f"   📊 Processando {uf} (Censo 2022)...")
    
    try:
        with zipfile.ZipFile(arquivo_zip, 'r') as z:
            # Encontrar CSV
            arquivos = [a for a in z.namelist() if a.endswith('.csv')]
            if not arquivos:
                print("   ⚠️  CSV não encontrado no ZIP 2022")
                return None
            
            arquivo_csv = arquivos[0]
            print(f"   📄 Lendo: {arquivo_csv}")
            
            # Ler CSV (Brasil todo)
            with z.open(arquivo_csv) as f:
                # Ler tudo de uma vez pode ser pesado (28MB zipado), mas CSV é ~100MB. Pandas aguenta.
                # Precisamos filtrar por UF. O código do setor começa com código da UF.
                # Ex: SP = 35...
                
                # Otimização: ler apenas colunas necessárias
                # CD_SETOR, V06004
                # Separator: ; (padrao IBGE)
                df = pd.read_csv(f, sep=';', encoding='latin-1', 
                               dtype={'CD_SETOR': str},
                               usecols=['CD_SETOR', 'V06004'],
                               decimal=',')
            
            # Filtrar UF
            cod_uf = UF_CODIGOS.get(uf)
            if not cod_uf:
                print(f"   ⚠️  Código UF não encontrado para {uf}")
                return None
            
            # Filtrar setores que começam com o código da UF
            df = df[df['CD_SETOR'].str.startswith(cod_uf)].copy()
            
            if len(df) == 0:
                print(f"   ⚠️  Nenhum setor encontrado para {uf} no arquivo BR")
                return None
            
            # Preparar resultado
            resultado = pd.DataFrame()
            resultado['codigo_setor'] = df['CD_SETOR']
            resultado['uf'] = uf
            
            # Colunas de contexto (Município)
            # No arquivo de renda só tem CD_SETOR. 
            # Podemos tentar extrair município do código do setor (7 primeiros dígitos)
            resultado['codigo_municipio'] = df['CD_SETOR'].str[:7]
            resultado['municipio'] = None # Não temos nome do município neste arquivo
            
            # Renda
            # V06004: Rendimento nominal médio mensal das pessoas responsáveis com rendimentos
            # Converter explicitamente substituindo vírgula (read_csv as vezes falha em detectar decimal se houver string misturada)
            series_renda = df['V06004'].astype(str).str.replace(',', '.')
            resultado['renda_responsavel_com_renda'] = pd.to_numeric(series_renda, errors='coerce')
            
            # Colunas vazias (não disponíveis em 2022 ou não compatíveis)
            resultado['renda_per_capita'] = None
            resultado['renda_media_domicilio'] = None
            resultado['log_renda_per_capita'] = None
            
            # Classes
            # Per Capita: NULL
            resultado['classe_renda_per_capita'] = None
            resultado['classe_renda_per_capita_nome'] = None
            
            # Responsável: Calculada sobre V06004
            resultado['classe_renda_responsavel'] = None
            resultado['classe_renda_responsavel_nome'] = None
            
            # Definir salário mínimo de 2022
            sm_2022 = config['salarios_minimos']['2022']
            
            for idx, row in resultado.iterrows():
                # Calculamos classe sobre a renda do responsável (sabendo que é métrica diferente)
                renda = row['renda_responsavel_com_renda']
                if pd.notna(renda) and renda > 0:
                    cl, nm = calcular_classe(renda, config, sm_2022)
                    resultado.at[idx, 'classe_renda_responsavel'] = cl
                    resultado.at[idx, 'classe_renda_responsavel_nome'] = nm
            
            # Remover inválidos
            resultado = resultado.dropna(subset=['renda_responsavel_com_renda'])
            
            # Metadados
            resultado['renda_quartil'] = None
            resultado['renda_decil'] = None
            resultado['salario_minimo_ref'] = sm_2022
            resultado['data_ref'] = '2022-08-01' # Censo 2022
            resultado['fonte'] = 'CENSO_2022'
            
            print(f"   ✅ {len(resultado):,} setores processados (2022)")
            return resultado

    except Exception as e:
        print(f"   ❌ Erro ao processar 2022: {e}")
        return None


def inserir_dados(df, conn):
    """Insere dados na tabela."""
    if df is None or len(df) == 0:
        return 0
    
    colunas = ['codigo_setor', 'uf', 'codigo_municipio', 'municipio', 
               'renda_media_domicilio', 'renda_per_capita', 'renda_responsavel_com_renda',
               'log_renda_per_capita', 
               'classe_renda_per_capita', 'classe_renda_per_capita_nome',
               'classe_renda_responsavel', 'classe_renda_responsavel_nome',
               'renda_quartil', 'renda_decil', 'salario_minimo_ref', 'data_ref', 'fonte']
    
    # Garantir colunas existem
    for col in colunas:
        if col not in df.columns:
            df[col] = None
    
    df_insert = df[colunas]
    
    buffer = StringIO()
    df_insert.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N')
    buffer.seek(0)
    
    with conn.cursor() as cur:
        cols_quoted = ', '.join([f'"{c}"' for c in colunas])
        cur.copy_expert(
            f"COPY {SCHEMA}.{TABELA} ({cols_quoted}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '\\N')",
            buffer
        )
    conn.commit()
    
    return len(df_insert)


# ============================================================
# MAIN
# ============================================================

def exportar_para_csv(df, nome_arquivo):
    """Exporta o DataFrame para CSV."""
    from config.settings import PROJECT_ROOT
    output_dir = PROJECT_ROOT / "output" / "censo_renda"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Define nome do arquivo se não passado
    output_file = output_dir / f"{nome_arquivo}.csv"
    
    # Selecionar colunas de interesse (mesmas do banco)
    colunas = ['codigo_setor', 'uf', 'codigo_municipio', 'municipio', 
               'renda_media_domicilio', 'renda_per_capita', 'renda_responsavel_com_renda',
               'log_renda_per_capita', 
               'classe_renda_per_capita', 'classe_renda_per_capita_nome',
               'classe_renda_responsavel', 'classe_renda_responsavel_nome',
               'renda_quartil', 'renda_decil', 'salario_minimo_ref', 'data_ref', 'fonte']
    
    # Garantir que colunas existam
    for col in colunas:
        if col not in df.columns:
            df[col] = None
            
    df[colunas].to_csv(output_file, index=False, sep=';', encoding='utf-8')
    print(f"   💾 Exportado: {output_file}")
    return len(df)


# ============================================================
# MAIN
# ============================================================

def baixar_e_processar_censo(tipo_saida="postgres"):
    """
    Função principal.
    Args:
        tipo_saida (str): 'postgres' ou 'csv'
    """
    print("=" * 70)
    print("💰 Carregando dados de Renda por Setor Censitário (Censo IBGE)")
    print("=" * 70)
    
    os.makedirs(PASTA_CACHE, exist_ok=True)
    
    # Carregar configuração de classes
    print("\n📋 Carregando configuração de classes...")
    config = carregar_config_classes()
    print(f"   ✅ SM 2010: R$ {config['salarios_minimos']['2010']:.2f}")
    print(f"   ✅ SM 2022: R$ {config['salarios_minimos']['2022']:.2f}")
    print(f"   ✅ Classes: {', '.join([c['classe'] for c in config['classes']])}")
    
    print(f"\n🗺️  Estados a processar: {', '.join(UFS)}")
    
    conn = None
    if tipo_saida == "postgres":
        print("\n🔌 Conectando ao PostgreSQL...")
        conn = psycopg2.connect(**DB_CONFIG)
        # Criar tabela
        criar_tabela(conn)
    else:
        print("\n💾 Modo CSV: Banco de dados não será acessado.")
    
    total_geral = 0
    
    # Processar cada UF
    for uf in UFS:
        print(f"\n{'='*50}")
        print(f"📍 Processando {uf} - Censo 2010")
        print("=" * 50)
        
        # Baixar (pode retornar lista para SP)
        arquivos_zip = baixar_censo_2010(uf)
        
        if not arquivos_zip:
            print(f"   ⚠️  Nenhum arquivo baixado para {uf}")
            continue
        
        # Processar cada arquivo
        for arquivo_zip in arquivos_zip:
            df = processar_censo_2010(arquivo_zip, uf, config)
            
            if df is not None:
                if tipo_saida == "postgres":
                    total = inserir_dados(df, conn)
                    print(f"   ✅ {arquivo_zip.name}: {total:,} setores inseridos")
                else:
                    total = exportar_para_csv(df, f"censo_2010_{arquivo_zip.stem}")
                    total_geral += total
        
        # ----------------------------------------------------
        # Processar Censo 2022 para a mesma UF
        # ----------------------------------------------------
        print(f"\n📍 Processando {uf} - Censo 2022")
        print("-" * 50)
        
        arquivo_zip_2022 = baixar_censo_2022(uf)
        if arquivo_zip_2022:
            df_2022 = processar_censo_2022(arquivo_zip_2022, uf, config)
            if df_2022 is not None:
                if tipo_saida == "postgres":
                    total = inserir_dados(df_2022, conn)
                    print(f"   ✅ Censo 2022: {total:,} setores inseridos")
                else:
                    total = exportar_para_csv(df_2022, f"censo_2022_{uf}")
                
                total_geral += total
    
    if conn:
        conn.close()
    
    print(f"\n{'='*70}")
    print("✅ Renda por setor carregada com sucesso!")
    print(f"   📊 Total de setores: {total_geral:,}")
    print(f"   🗄️  Tabela: {SCHEMA}.{TABELA}")
    
    print(f"""
{'='*70}
💡 COMO USAR:
{'='*70}

-- Consultar renda por setor:
SELECT codigo_setor, classe_renda, renda_per_capita
FROM {SCHEMA}.{TABELA}
LIMIT 10;

-- JOIN com internações via lookup:
SELECT 
    i.diagnostico_principal,
    r.classe_renda,
    COUNT(*) as total,
    AVG(i.valor_total) as valor_medio
FROM saude.vw_internacoes_geo i
JOIN {SCHEMA}.cep_geocodificado g ON REPLACE(i.cep_paciente, '-', '') = g.cep
JOIN {SCHEMA}.{TABELA} r ON g.codigo_setor = r.codigo_setor
GROUP BY i.diagnostico_principal, r.classe_renda
ORDER BY total DESC;

-- Taxa de óbito por classe de renda:
SELECT 
    r.classe_renda,
    r.classe_renda_nome,
    COUNT(*) as total,
    SUM(CASE WHEN i.obito = 'S' THEN 1 ELSE 0 END) as obitos,
    ROUND(100.0 * SUM(CASE WHEN i.obito = 'S' THEN 1 ELSE 0 END) / COUNT(*), 2) as taxa_obito
FROM saude.vw_internacoes_geo i
JOIN {SCHEMA}.cep_geocodificado g ON REPLACE(i.cep_paciente, '-', '') = g.cep
JOIN {SCHEMA}.{TABELA} r ON g.codigo_setor = r.codigo_setor
GROUP BY r.classe_renda, r.classe_renda_nome
ORDER BY r.classe_renda;

{'='*70}
    """)


if __name__ == "__main__":
    baixar_e_processar_censo()
