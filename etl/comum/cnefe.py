"""
Script para carregar endereços geocodificados do CNEFE (IBGE Censo 2022).
Base com ~106 milhões de endereços para todo o Brasil.

Permite geocodificar CEPs com precisão de endereço (lat/long por número).

Configuração:
    UFS = ["SP"]  # Adicione mais estados conforme necessário

Uso:
    source venv/bin/activate
    python -m etl.comum.cnefe
"""

import os
import requests
import pandas as pd
import psycopg2
from io import StringIO
from pathlib import Path
from zipfile import ZipFile
from tqdm import tqdm

# --- CONFIGURAÇÕES ---
# Conexão PostgreSQL e Configuração Modular
from config.settings import DB_CONFIG, ETL_CONFIG

# Obtém configs específicas ou usa defaults
CONF = ETL_CONFIG.get("cnefe", {})
UFS = CONF.get("ufs", ETL_CONFIG["padrao"]["ufs"])

PASTA_CACHE = "cache/cnefe"
SCHEMA = "comum"
TABELA = "cnefe_enderecos"

# URL base para download dos arquivos CNEFE (IBGE)
# Atualizado em 2024 - nova estrutura de pastas
URL_BASE = "https://ftp.ibge.gov.br/Cadastro_Nacional_de_Enderecos_para_Fins_Estatisticos/Censo_Demografico_2022/Arquivos_CNEFE/CSV/UF"

# Código IBGE das UFs (para montar nome do arquivo)
UF_CODIGOS = {
    "RO": "11", "AC": "12", "AM": "13", "RR": "14", "PA": "15",
    "AP": "16", "TO": "17", "MA": "21", "PI": "22", "CE": "23",
    "RN": "24", "PB": "25", "PE": "26", "AL": "27", "SE": "28",
    "BA": "29", "MG": "31", "ES": "32", "RJ": "33", "SP": "35",
    "PR": "41", "SC": "42", "RS": "43", "MS": "50", "MT": "51",
    "GO": "52", "DF": "53"
}

# Colunas do CNEFE (baseado no dicionário do IBGE)
COLUNAS_CNEFE = [
    'COD_UNICO_ENDERECO', 'UF', 'COD_MUNICIPIO', 'COD_DISTRITO', 'COD_SUBDISTRITO',
    'COD_SETOR', 'NUM_QUADRA', 'NUM_FACE', 'CEP', 'DSC_LOCALIDADE',
    'NOM_TIPO_SEGLOGam', 'NOM_TITULO_SEGLOGAM', 'NOM_SEGLOGAM', 'NUM_ENDERECO',
    'DSC_MODIFICADOR', 'NOM_COMP_ELEM1', 'VAL_COMP_ELEM1', 'NOM_COMP_ELEM2',
    'VAL_COMP_ELEM2', 'NOM_COMP_ELEM3', 'VAL_COMP_ELEM3', 'NOM_COMP_ELEM4',
    'VAL_COMP_ELEM4', 'NOM_COMP_ELEM5', 'VAL_COMP_ELEM5', 'LATITUDE', 'LONGITUDE',
    'NV_GEO_COORD', 'COD_ESPECIE', 'DSC_ESTABELECIMENTO'
]

# Mapeamento para nomes intuitivos
MAPEAMENTO_COLUNAS = {
    'COD_UNICO_ENDERECO': 'codigo_endereco',
    'UF': 'uf',
    'COD_MUNICIPIO': 'codigo_municipio',
    'COD_DISTRITO': 'codigo_distrito',
    'COD_SUBDISTRITO': 'codigo_subdistrito',
    'COD_SETOR': 'codigo_setor',
    'NUM_QUADRA': 'numero_quadra',
    'NUM_FACE': 'numero_face',
    'CEP': 'cep',
    'DSC_LOCALIDADE': 'localidade',
    'NOM_TIPO_SEGLOGAM': 'tipo_logradouro',
    'NOM_TITULO_SEGLOGAM': 'titulo_logradouro',
    'NOM_SEGLOGAM': 'logradouro',
    'NUM_ENDERECO': 'numero',
    'DSC_MODIFICADOR': 'modificador',
    'NOM_COMP_ELEM1': 'complemento_tipo1',
    'VAL_COMP_ELEM1': 'complemento_valor1',
    'LATITUDE': 'latitude',
    'LONGITUDE': 'longitude',
    'NV_GEO_COORD': 'nivel_geocodificacao',
    'COD_ESPECIE': 'codigo_especie',
    'DSC_ESTABELECIMENTO': 'estabelecimento'
}


def baixar_arquivo(url, destino):
    """Baixa arquivo com barra de progresso."""
    response = requests.get(url, stream=True)
    response.raise_for_status()
    
    total = int(response.headers.get('content-length', 0))
    
    with open(destino, 'wb') as f:
        with tqdm(total=total, unit='B', unit_scale=True, desc=f"   📥 Baixando") as pbar:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                pbar.update(len(chunk))


def configurar_extensoes(conn):
    """Configura extensões necessárias do PostgreSQL."""
    with conn.cursor() as cur:
        # Extensão para fuzzy matching (trigram)
        try:
            cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
            print("   ✅ Extensão pg_trgm ativada")
        except Exception as e:
            print(f"   ⚠️  pg_trgm: {e}")
        
        conn.commit()


def criar_tabela(conn):
    """Cria a tabela de endereços CNEFE PARTICIONADA por UF."""
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
        
        # Drop se existir (para recarga completa)
        cur.execute(f"DROP TABLE IF EXISTS {SCHEMA}.{TABELA} CASCADE")
        
        # Tabela PARTICIONADA por UF
        cur.execute(f"""
            CREATE TABLE {SCHEMA}.{TABELA} (
                id BIGSERIAL,
                uf CHAR(2) NOT NULL,
                codigo_setor VARCHAR(20),
                cep CHAR(8),
                logradouro TEXT,
                numero TEXT,
                bairro TEXT,
                municipio TEXT,
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                data_ref DATE DEFAULT '2022-08-01',
                PRIMARY KEY (id, uf)
            ) PARTITION BY LIST (uf);
            
            COMMENT ON TABLE {SCHEMA}.{TABELA} IS 
                'Endereços geocodificados do CNEFE (Censo 2022 IBGE). PARTICIONADA por UF.
                 codigo_setor: para JOIN com dados de renda do Censo.
                 data_ref: data de referência da coleta (ago/2022).';
        """)
        conn.commit()
        print(f"   ✅ Tabela {SCHEMA}.{TABELA} criada (PARTICIONADA por UF)")


def criar_particao_cnefe(conn, uf):
    """Cria partição para uma UF se não existir."""
    nome_particao = f"{TABELA}_{uf.lower()}"
    
    with conn.cursor() as cur:
        # Verifica se partição existe
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM pg_tables 
                WHERE schemaname = %s AND tablename = %s
            )
        """, (SCHEMA, nome_particao))
        existe = cur.fetchone()[0]
        
        if not existe:
            cur.execute(f"""
                CREATE TABLE {SCHEMA}.{nome_particao}
                PARTITION OF {SCHEMA}.{TABELA}
                FOR VALUES IN ('{uf}')
            """)
            conn.commit()


def criar_indices(conn):
    """Cria índices otimizados para busca (herdados pelas partições)."""
    print("\n📊 Criando índices (pode demorar alguns minutos)...")
    
    # Em tabelas particionadas, índices são criados na tabela pai
    # e automaticamente herdados por cada partição
    # Não precisa índice de UF pois o particionamento já otimiza isso
    indices = [
        (f"idx_{TABELA}_cep", f"CREATE INDEX idx_{TABELA}_cep ON {SCHEMA}.{TABELA}(cep)"),
        (f"idx_{TABELA}_setor", f"CREATE INDEX idx_{TABELA}_setor ON {SCHEMA}.{TABELA}(codigo_setor)"),
        (f"idx_{TABELA}_logradouro", f"CREATE INDEX idx_{TABELA}_logradouro ON {SCHEMA}.{TABELA} USING gin (logradouro gin_trgm_ops)"),
        (f"idx_{TABELA}_coords", f"CREATE INDEX idx_{TABELA}_coords ON {SCHEMA}.{TABELA}(latitude, longitude) WHERE latitude IS NOT NULL"),
    ]
    
    with conn.cursor() as cur:
        for nome, sql in tqdm(indices, desc="   Índices"):
            try:
                cur.execute(sql)
                conn.commit()
            except Exception as e:
                print(f"   ⚠️  {nome}: {e}")
                conn.rollback()
    
    print("   ✅ Índices criados")


def criar_funcao_geocodificacao(conn):
    """Cria função de geocodificação em cascata."""
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE OR REPLACE FUNCTION {SCHEMA}.geocodificar(
                p_cep TEXT,
                p_numero TEXT DEFAULT NULL
            ) RETURNS TABLE(
                lat DOUBLE PRECISION, 
                lng DOUBLE PRECISION, 
                precisao TEXT,
                logradouro_encontrado TEXT
            ) AS $$
            BEGIN
                -- Normaliza CEP (remove hífen)
                p_cep := REPLACE(p_cep, '-', '');
                p_cep := LPAD(p_cep, 8, '0');
                
                -- 1º: CEP + Número exato (precisão máxima)
                IF p_numero IS NOT NULL AND p_numero != '' THEN
                    RETURN QUERY 
                    SELECT latitude, longitude, 'ENDERECO_EXATO'::TEXT, logradouro::TEXT
                    FROM {SCHEMA}.{TABELA}
                    WHERE cep = p_cep AND numero = p_numero
                    AND latitude IS NOT NULL
                    LIMIT 1;
                    IF FOUND THEN RETURN; END IF;
                END IF;
                
                -- 2º: Média do CEP (centroide do logradouro)
                RETURN QUERY 
                SELECT AVG(latitude), AVG(longitude), 'CEP_MEDIA'::TEXT, 
                       MAX(logradouro)::TEXT
                FROM {SCHEMA}.{TABELA}
                WHERE cep = p_cep AND latitude IS NOT NULL
                HAVING COUNT(*) > 0;
                IF FOUND THEN RETURN; END IF;
                
                -- 3º: Fallback para tabela de municípios
                RETURN QUERY 
                SELECT m.latitude, m.longitude, 'MUNICIPIO'::TEXT, 
                       c.logradouro::TEXT
                FROM {SCHEMA}.cep_logradouros c
                JOIN {SCHEMA}.municipios_coordenadas m 
                    ON c.cidade = m.municipio AND c.uf = m.uf
                WHERE c.cep = p_cep
                LIMIT 1;
            END;
            $$ LANGUAGE plpgsql;
            
            COMMENT ON FUNCTION {SCHEMA}.geocodificar IS 
                'Geocodifica um CEP retornando lat/long com precisão em cascata:
                 1º ENDERECO_EXATO: CEP + número específico
                 2º CEP_MEDIA: centroide de todos os endereços do CEP
                 3º MUNICIPIO: centroide do município (fallback)';
        """)
        conn.commit()
        print(f"   ✅ Função {SCHEMA}.geocodificar() criada")


def processar_csv_em_chunks(arquivo_csv, conn, uf, tipo_saida="postgres", chunk_size=100000):
    """Processa CSV grande em chunks (versão otimizada)."""
    
    # Colunas reais do CSV do IBGE
    colunas_usar = ['COD_UF', 'COD_SETOR', 'COD_MUNICIPIO', 'CEP', 'NOM_SEGLOGR', 
                    'NUM_ENDERECO', 'DSC_LOCALIDADE', 'LATITUDE', 'LONGITUDE']
    
    # Mapeamento para nomes intuitivos
    renomear = {
        'COD_UF': 'uf',
        'COD_SETOR': 'codigo_setor',
        'COD_MUNICIPIO': 'municipio',
        'CEP': 'cep',
        'NOM_SEGLOGR': 'logradouro',
        'NUM_ENDERECO': 'numero',
        'DSC_LOCALIDADE': 'bairro',
        'LATITUDE': 'latitude',
        'LONGITUDE': 'longitude'
    }
    
    # Carrega mapeamento apenas se for postgres (para preencher nomes)
    # Se for CSV, vamos precisar carregar de algum lugar ou deixar sem nome?
    # Melhor carregar do banco se possível, ou usar arquivo fixo.
    # Assumindo que se for CSV, pode não ter conexão.
    # Para simplificar: se tipo_saida=csv, vamos tentar conectar só pra ler o mapa, 
    # se falhar, fica sem nome do município.
    
    mapa_municipios = {}
    if conn:
        print("   📍 Carregando mapeamento de municípios...")
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT codigo_ibge, municipio FROM comum.municipios_coordenadas 
                    WHERE codigo_ibge IS NOT NULL
                """)
                for cod, nome in cur.fetchall():
                    # Remove .0 do código se existir
                    cod_limpo = str(cod).replace('.0', '')
                    mapa_municipios[cod_limpo] = nome
            print(f"   ✅ {len(mapa_municipios)} municípios mapeados")
        except Exception as e:
            print(f"   ⚠️  Tabela de municípios não disponível: {type(e).__name__}")
            print(f"      Nomes de municípios ficarão em branco (dados geocodificados serão carregados normalmente)")
            mapa_municipios = {}
            # Rollback para limpar o estado de erro
            conn.rollback()
    
    # Preparar arquivo de saída CSV (se aplicável)
    arquivo_saida_csv = None
    if tipo_saida == "csv":
        from config.settings import PROJECT_ROOT
        output_dir = PROJECT_ROOT / "output" / "cnefe"
        output_dir.mkdir(parents=True, exist_ok=True)
        arquivo_saida_csv = output_dir / f"cnefe_{uf}.csv"
        # Limpa arquivo se existir
        if arquivo_saida_csv.exists():
            os.remove(arquivo_saida_csv)
            
    total_inseridos = 0
    
    # Processa em chunks
    for i, chunk in enumerate(tqdm(pd.read_csv(arquivo_csv, sep=';', encoding='latin-1', 
                                   chunksize=chunk_size, low_memory=False,
                                   usecols=lambda c: c in colunas_usar,
                                   on_bad_lines='skip'),
                      desc="   Processando")):
        
        # Renomeia colunas
        df = chunk.rename(columns=renomear)
        
        # Converte código UF numérico para sigla (35 -> SP)
        cod_para_uf = {v: k for k, v in UF_CODIGOS.items()}
        df['uf'] = df['uf'].astype(str).map(cod_para_uf).fillna(uf)
        
        # Converte código de município para nome
        if mapa_municipios:
            df['municipio'] = df['municipio'].astype(str).map(mapa_municipios)
        
        # Formata CEP
        if 'cep' in df.columns:
            df['cep'] = df['cep'].astype(str).str.replace('.0', '', regex=False).str.zfill(8).str[:8]
        
        # Converte coordenadas
        df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
        
        # Remove linhas sem coordenadas
        df = df.dropna(subset=['latitude', 'longitude'])
        
        if 'bairro' not in df.columns: df['bairro'] = None
        
        # Colunas finais
        colunas_insert = ['uf', 'codigo_setor', 'cep', 'logradouro', 'numero', 'bairro', 
                          'municipio', 'latitude', 'longitude']
        
        # Garante que todas colunas existem no DF
        for col in colunas_insert:
            if col not in df.columns:
                df[col] = None
                
        df_final = df[colunas_insert]
        
        if tipo_saida == "postgres":
            # Cria partição para UF se não existir
            criar_particao_cnefe(conn, uf)
            
            from io import StringIO
            buffer = StringIO()
            df_final.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N')
            buffer.seek(0)
            
            with conn.cursor() as cur:
                cols_quoted = ', '.join([f'"{c}"' for c in colunas_insert])
                cur.copy_expert(
                    f"COPY {SCHEMA}.{TABELA} ({cols_quoted}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '\\N')",
                    buffer
                )
            conn.commit()
            
        else: # CSV
            # Append mode
            header = (i == 0) # Escreve header apenas no primeiro chunk
            df_final.to_csv(arquivo_saida_csv, mode='a', index=False, sep=';', header=header, encoding='utf-8')
            
        total_inseridos += len(df_final)
        
    return total_inseridos


def verificar_tabela_municipios(conn):
    """Verifica se a tabela comum.municipios_coordenadas existe."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM pg_tables
                    WHERE schemaname = 'comum' AND tablename = 'municipios_coordenadas'
                );
            """)
            return cur.fetchone()[0]
    except Exception:
        return False

def baixar_arquivo_cnefe(uf):
    """Baixa o arquivo ZIP do CNEFE para uma UF específica."""
    codigo_uf = UF_CODIGOS.get(uf, "35")
    nome_arquivo_zip_ibge = f"{codigo_uf}_{uf}.zip"
    arquivo_zip_local = Path(PASTA_CACHE) / f"CNEFE_{uf}.zip"
    
    url = f"{URL_BASE}/{nome_arquivo_zip_ibge}"
    
    if not arquivo_zip_local.exists():
        print(f"\n📥 Baixando de {url}...")
        try:
            baixar_arquivo(url, arquivo_zip_local)
            return arquivo_zip_local
        except Exception as e:
            print(f"   ❌ Erro ao baixar {uf}: {e}")
            return None
    else:
        print(f"\n   ✅ Arquivo ZIP para {uf} já existe.")
        return arquivo_zip_local

def tabela_tem_dados(conn, uf):
    """Verifica se a tabela já contém dados para a UF."""
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {SCHEMA}.{TABELA} WHERE uf = %s", (uf,))
            return cur.fetchone()[0] > 0
    except Exception:
        return False


def baixar_e_processar(tipo_saida="postgres"):
    """
    Função principal.
    Args:
        tipo_saida (str): 'postgres' ou 'csv'
    """
    os.makedirs(PASTA_CACHE, exist_ok=True)
    
    print("=" * 60)
    print("🌍 Carregando Endereços do CNEFE (IBGE 2022)")
    print("=" * 60)
    
    conn = None
    try:
        # Tenta conectar no banco (necessário para mapa de municípios e insert normal)
        # Se tipo_saida=csv e banco falhar, segue sem mapa de municipios
        conn = psycopg2.connect(**DB_CONFIG)
        if tipo_saida == "postgres":
            print("🔌 Conectado ao PostgreSQL.")
            # Garante tabelas se for banco
            criar_tabela(conn)
            if not verificar_tabela_municipios(conn):
                print("⚠️  Tabela de municípios não encontrada. Nomes de cidades podem ficar em branco.")
        else:
            print("🔌 Conectado ao PostgreSQL (apenas para leitura de metadados).")
            
    except Exception as e:
        if tipo_saida == "postgres":
            print(f"❌ Erro crítico: Não foi possível conectar ao banco: {e}")
            return
        else:
            print(f"⚠️  Sem conexão com banco ({e}). Exportação CSV será feita sem nomes de municípios.")
            conn = None
            
    total_geral = 0
    
    for uf in UFS:
        print(f"\n📍 Processando Estado: {uf}")
        
        # Baixa arquivo
        arquivo_zip = baixar_arquivo_cnefe(uf)
        if not arquivo_zip:
            continue
            
        # Descompacta e encontra CSV
        with ZipFile(arquivo_zip, 'r') as z:
            csv_files = [f for f in z.namelist() if f.lower().endswith('.csv')]
            if not csv_files:
                print(f"   ❌ Nenhum CSV encontrado no ZIP de {uf}")
                continue
                
            csv_nome = csv_files[0]
            print(f"   📦 Extraindo {csv_nome}...")
            z.extract(csv_nome, PASTA_CACHE)
            csv_path = Path(PASTA_CACHE) / csv_nome
            
            try:
                # Processa
                if tipo_saida == "postgres":
                    if tabela_tem_dados(conn, uf):
                         # Limpa dados anteriores da UF
                        print(f"   🔄 Limpando dados anteriores de {uf}...")
                        with conn.cursor() as cur:
                            cur.execute(f"DELETE FROM {SCHEMA}.{TABELA} WHERE uf = %s", (uf,))
                        conn.commit()
                
                total = processar_csv_em_chunks(csv_path, conn, uf, tipo_saida=tipo_saida)
                total_geral += total
                print(f"   ✅ {uf}: {total:,} endereços inseridos")
                
            except Exception as e:
                print(f"   ❌ Erro em {uf}: {e}")
                import traceback
                traceback.print_exc()
    
    # Cria índices
    criar_indices(conn)
    
    # Cria função de geocodificação
    criar_funcao_geocodificacao(conn)
    
    # Cria tabela de lookup para JOINs rápidos
    criar_tabela_lookup(conn)
    
    # Cria VIEW para internações geocodificadas
    criar_view_internacoes(conn)
    
    conn.close()
    
    print(f"\n{'='*70}")
    print("✅ CNEFE carregado com sucesso!")
    print(f"   📊 Total de endereços: {total_geral:,}")
    print(f"   🗄️  Tabela: {SCHEMA}.{TABELA}")
    print(f"   🔍 Lookup: {SCHEMA}.cep_geocodificado")
    print(f"   📊 VIEW: saude.vw_internacoes_geo")
    
    print(f"""
{'='*70}
💡 COMO USAR:
{'='*70}

-- Consulta rápida de internações geocodificadas:
SELECT numero_aih, cep_paciente, municipio_geo, latitude, longitude
FROM saude.vw_internacoes_geo
WHERE latitude IS NOT NULL
LIMIT 100;

-- Mapa de calor por diagnóstico:
SELECT diagnostico_principal, AVG(latitude) as lat, AVG(longitude) as lng, COUNT(*)
FROM saude.vw_internacoes_geo
WHERE latitude IS NOT NULL
GROUP BY diagnostico_principal;

-- Geocodificar um CEP específico:
SELECT * FROM {SCHEMA}.cep_geocodificado WHERE cep = '01310100';

{'='*70}
    """)


def criar_tabela_lookup(conn):
    """Cria tabela de lookup pré-agregada por CEP para JOINs rápidos."""
    print("\n📦 Criando tabela de lookup...")
    
    with conn.cursor() as cur:
        cur.execute(f"""
            DROP TABLE IF EXISTS {SCHEMA}.cep_geocodificado CASCADE;
            
            CREATE TABLE {SCHEMA}.cep_geocodificado AS
            SELECT 
                cep,
                MODE() WITHIN GROUP (ORDER BY codigo_setor) as codigo_setor,
                LEFT(MODE() WITHIN GROUP (ORDER BY codigo_setor), 15) as codigo_setor_base,
                MAX(logradouro) as logradouro,
                MAX(bairro) as bairro,
                MAX(municipio) as municipio,
                MAX(uf) as uf,
                AVG(latitude) as latitude,
                AVG(longitude) as longitude,
                COUNT(*) as total_enderecos
            FROM {SCHEMA}.{TABELA}
            WHERE cep IS NOT NULL AND latitude IS NOT NULL
            GROUP BY cep;
            
            CREATE UNIQUE INDEX idx_cep_geo ON {SCHEMA}.cep_geocodificado(cep);
            CREATE INDEX idx_cep_geo_setor ON {SCHEMA}.cep_geocodificado(codigo_setor);
            CREATE INDEX idx_cep_geo_setor_base ON {SCHEMA}.cep_geocodificado(codigo_setor_base);
            
            COMMENT ON TABLE {SCHEMA}.cep_geocodificado IS 
                'Tabela de lookup com coordenadas por CEP (pré-agregada). Use para JOINs rápidos com internações e dados do Censo.';
            COMMENT ON COLUMN {SCHEMA}.cep_geocodificado.codigo_setor IS 
                'Código do setor censitário do CNEFE 2022 (com sufixo P)';
            COMMENT ON COLUMN {SCHEMA}.cep_geocodificado.codigo_setor_base IS 
                'Código do setor sem sufixo P (15 dígitos). USE PARA JOIN com comum.setor_renda!';
        """)
        conn.commit()
        
        cur.execute(f"SELECT COUNT(*) FROM {SCHEMA}.cep_geocodificado")
        total = cur.fetchone()[0]
        print(f"   ✅ {total:,} CEPs únicos no lookup (com codigo_setor_base)")


def criar_view_internacoes(conn):
    """Cria VIEW para internações geocodificadas."""
    print("\n🔗 Criando VIEW de internações geocodificadas...")
    
    with conn.cursor() as cur:
        cur.execute(f"""
            DROP VIEW IF EXISTS saude.vw_internacoes_geo CASCADE;
            
            CREATE VIEW saude.vw_internacoes_geo AS
            SELECT 
                i.*,
                g.logradouro AS logradouro_geo,
                g.bairro AS bairro_geo,
                g.municipio AS municipio_geo,
                g.latitude,
                g.longitude,
                g.total_enderecos,
                CASE 
                    WHEN g.cep IS NOT NULL THEN 'CEP_ENCONTRADO'
                    ELSE 'NAO_ENCONTRADO'
                END AS precisao_geocodificacao
            FROM saude.internacoes i
            LEFT JOIN {SCHEMA}.cep_geocodificado g 
                ON REPLACE(i.cep_paciente, '-', '') = g.cep;
            
            COMMENT ON VIEW saude.vw_internacoes_geo IS 
                'Internações com geocodificação via tabela lookup (rápido!).
                 Colunas adicionadas: latitude, longitude, municipio_geo, precisao_geocodificacao.';
        """)
        conn.commit()
        print("   ✅ VIEW saude.vw_internacoes_geo criada")


if __name__ == "__main__":
    carregar_cnefe()

