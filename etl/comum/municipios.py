#!/usr/bin/env python3
"""
ETL - Carregar municípios brasileiros com coordenadas do IBGE.

Fonte: API pública do IBGE (servicodados.ibge.gov.br)
Não depende de arquivos externos!

Uso:
    python -m etl.comum.municipios
    
Saída:
    Tabela: comum.municipios_coordenadas (~5.600 municípios)
"""

import sys
import requests
import psycopg2
from pathlib import Path

# Adiciona raiz do projeto ao path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from config.settings import DB_CONFIG

SCHEMA = "comum"
TABELA = "municipios_coordenadas"

# APIs do IBGE
URL_MUNICIPIOS = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
URL_GEOJSON_UF = "https://servicodados.ibge.gov.br/api/v3/malhas/estados/{uf}?formato=application/vnd.geo+json&qualidade=minima&intrarregiao=municipio"


def criar_tabela(conn):
    """Cria a tabela de municípios com coordenadas."""
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
        cur.execute(f"DROP TABLE IF EXISTS {SCHEMA}.{TABELA} CASCADE")
        
        cur.execute(f"""
            CREATE TABLE {SCHEMA}.{TABELA} (
                id SERIAL PRIMARY KEY,
                codigo_ibge VARCHAR(7) NOT NULL UNIQUE,
                municipio TEXT NOT NULL,
                uf CHAR(2) NOT NULL,
                uf_nome TEXT,
                mesorregiao TEXT,
                microrregiao TEXT,
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                data_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE INDEX idx_{TABELA}_codigo ON {SCHEMA}.{TABELA}(codigo_ibge);
            CREATE INDEX idx_{TABELA}_uf ON {SCHEMA}.{TABELA}(uf);
            CREATE INDEX idx_{TABELA}_nome ON {SCHEMA}.{TABELA}(municipio);
            
            COMMENT ON TABLE {SCHEMA}.{TABELA} IS 
                'Municípios brasileiros com coordenadas (centroide). Fonte: API IBGE.';
        """)
        conn.commit()
        print(f"   ✅ Tabela {SCHEMA}.{TABELA} criada")


def baixar_municipios():
    """Baixa lista de municípios da API do IBGE."""
    print("   📥 Baixando lista de municípios...")
    
    response = requests.get(URL_MUNICIPIOS, timeout=60)
    response.raise_for_status()
    
    municipios = response.json()
    print(f"   ✅ {len(municipios)} municípios encontrados")
    
    return municipios


def baixar_coordenadas_por_uf(uf_codigo):
    """Baixa coordenadas (centroide) dos municípios de uma UF via GeoJSON."""
    url = URL_GEOJSON_UF.format(uf=uf_codigo)
    
    try:
        response = requests.get(url, timeout=120)
        response.raise_for_status()
        geojson = response.json()
        
        # Extrai centroide de cada município
        coordenadas = {}
        for feature in geojson.get('features', []):
            cod = feature.get('properties', {}).get('codarea')
            if cod and feature.get('geometry'):
                coords = feature['geometry'].get('coordinates', [])
                if coords:
                    try:
                        if feature['geometry']['type'] == 'Polygon':
                            pontos = coords[0]
                        elif feature['geometry']['type'] == 'MultiPolygon':
                            pontos = coords[0][0]
                        else:
                            pontos = coords
                        
                        if pontos and len(pontos) > 0:
                            lons = [p[0] for p in pontos if len(p) >= 2]
                            lats = [p[1] for p in pontos if len(p) >= 2]
                            if lons and lats:
                                coordenadas[cod] = {
                                    'longitude': sum(lons) / len(lons),
                                    'latitude': sum(lats) / len(lats)
                                }
                    except (IndexError, TypeError):
                        pass
        
        return coordenadas
        
    except Exception as e:
        print(f"   ⚠️  Erro ao baixar coordenadas UF {uf_codigo}: {e}")
        return {}


def carregar_municipios(tipo_saida="postgres"):
    """Função principal."""
    print("=" * 60)
    print("🏙️  Carregando Municípios Brasileiros (API IBGE)")
    print("=" * 60)
    
    # Baixa lista de municípios
    municipios = baixar_municipios()
    
    # Agrupa por UF para baixar coordenadas
    ufs = {}
    for m in municipios:
        try:
            microrregiao = m.get('microrregiao') or {}
            mesorregiao = microrregiao.get('mesorregiao') or {}
            uf_info = mesorregiao.get('UF') or {}
            
            uf_sigla = uf_info.get('sigla')
            uf_codigo = uf_info.get('id')
            
            if uf_codigo and uf_sigla and uf_codigo not in ufs:
                ufs[uf_codigo] = uf_sigla
        except Exception:
            continue
    
    # Baixa coordenadas por UF
    print(f"\n   📍 Baixando coordenadas de {len(ufs)} UFs...")
    todas_coordenadas = {}
    for uf_codigo, uf_sigla in ufs.items():
        coords = baixar_coordenadas_por_uf(uf_codigo)
        todas_coordenadas.update(coords)
        if coords:
            print(f"      ✅ {uf_sigla}: {len(coords)} municípios")
    
    print(f"   ✅ Total de coordenadas: {len(todas_coordenadas)}")
    
    # Prepara dados para inserção
    dados = []
    for m in municipios:
        try:
            codigo = str(m['id'])
            coord = todas_coordenadas.get(codigo, {})
            
            microrregiao = m.get('microrregiao') or {}
            mesorregiao = microrregiao.get('mesorregiao') or {}
            uf_info = mesorregiao.get('UF') or {}
            
            dados.append({
                'codigo_ibge': codigo,
                'municipio': m.get('nome', ''),
                'uf': uf_info.get('sigla', ''),
                'uf_nome': uf_info.get('nome', ''),
                'mesorregiao': mesorregiao.get('nome', ''),
                'microrregiao': microrregiao.get('nome', ''),
                'latitude': coord.get('latitude'),
                'longitude': coord.get('longitude')
            })
        except Exception:
            continue
    
    if tipo_saida == "postgres":
        print("\n🔌 Conectando ao PostgreSQL...")
        conn = psycopg2.connect(**DB_CONFIG)
        criar_tabela(conn)
        
        # Insere os dados usando executemany para performance
        print("   📊 Inserindo dados...")
        with conn.cursor() as cur:
            for d in dados:
                cur.execute(f"""
                    INSERT INTO {SCHEMA}.{TABELA} 
                    (codigo_ibge, municipio, uf, uf_nome, mesorregiao, microrregiao, latitude, longitude)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    d['codigo_ibge'],
                    d['municipio'],
                    d['uf'],
                    d['uf_nome'],
                    d['mesorregiao'],
                    d['microrregiao'],
                    d['latitude'],
                    d['longitude']
                ))
        conn.commit()
        conn.close()
        
        print(f"\n✅ {len(dados)} municípios carregados em {SCHEMA}.{TABELA}")
        
    else:
        # Exporta para CSV
        from config.settings import PROJECT_ROOT
        import pandas as pd
        
        output_dir = PROJECT_ROOT / "output" / "municipios"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / "municipios_coordenadas.csv"
        
        df = pd.DataFrame(dados)
        df.to_csv(output_file, index=False, sep=';', encoding='utf-8')
        print(f"\n💾 Exportado: {output_file}")


if __name__ == "__main__":
    carregar_municipios()
