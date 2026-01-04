"""
Script para carregar/atualizar tabela IPCA no schema comum.
O IPCA é usado para correção monetária em todos os schemas.

Uso:
    source venv/bin/activate
    python carregar_ipca.py
"""

import pandas as pd
import psycopg2
from datetime import datetime

from config.settings import DB_CONFIG

SCHEMA = "comum"


def carregar_ipca():
    """Baixa e insere a tabela IPCA do Banco Central."""
    print("=" * 50)
    print("💰 Carregando IPCA (Banco Central do Brasil)")
    print("=" * 50)
    
    SERIE_IPCA = 433
    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{SERIE_IPCA}/dados?formato=json"
    
    try:
        print("\n📡 Baixando dados do BCB...")
        df = pd.read_json(url)
        df["data"] = pd.to_datetime(df["data"], format="%d/%m/%Y", errors="coerce")
        df["ano"] = df["data"].dt.year.astype("int")
        df["mes"] = df["data"].dt.month.astype("int")
        df["indice_sgs"] = pd.to_numeric(df["valor"], errors="coerce")
        
        df = df.dropna(subset=["ano", "mes", "indice_sgs"])
        df = df[["ano", "mes", "indice_sgs"]]
        
        # Calcula fator deflator
        base_ano = df["ano"].max()
        base_mes = df[df["ano"] == base_ano]["mes"].max()
        indice_base = df[(df["ano"] == base_ano) & (df["mes"] == base_mes)]["indice_sgs"].values[0]
        
        df["fator_deflator"] = indice_base / df["indice_sgs"]
        df["periodo_base"] = f"{base_ano}-{base_mes:02d}"
        
        print(f"   ✅ {len(df):,} meses baixados")
        print(f"   📅 Período: 1994 a {base_ano}")
        print(f"   🎯 Base para deflação: {base_ano}-{base_mes:02d}")
        
        # Conecta e insere
        print("\n💾 Salvando no banco...")
        conn = psycopg2.connect(**DB_CONFIG)
        
        with conn.cursor() as cur:
            # Garante que o schema existe
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
            
            cur.execute(f"""
                DROP TABLE IF EXISTS {SCHEMA}.ipca_mensal CASCADE;
                
                CREATE TABLE {SCHEMA}.ipca_mensal (
                    ano INTEGER NOT NULL,
                    mes INTEGER NOT NULL,
                    indice_sgs DOUBLE PRECISION,
                    fator_deflator DOUBLE PRECISION,
                    periodo_base VARCHAR(10),
                    PRIMARY KEY (ano, mes)
                );
                
                COMMENT ON TABLE {SCHEMA}.ipca_mensal IS 
                    'IPCA mensal para correção monetária. Fonte: BCB Série 433';
            """)
            conn.commit()
            
            from io import StringIO
            buffer = StringIO()
            df.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N')
            buffer.seek(0)
            
            cur.copy_expert(
                f"COPY {SCHEMA}.ipca_mensal FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '\\N')",
                buffer
            )
            conn.commit()
        
        conn.close()
        
        print(f"   ✅ Tabela {SCHEMA}.ipca_mensal atualizada!")
        print("\n" + "=" * 50)
        
    except Exception as e:
        print(f"❌ Erro: {e}")


if __name__ == "__main__":
    carregar_ipca()
