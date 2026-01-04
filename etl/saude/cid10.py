"""
Script para carregar/atualizar tabela CID-10 no schema saude.
Classificação Internacional de Doenças da OMS.

Uso:
    source venv/bin/activate
    python carregar_cid10.py
"""

import pandas as pd
import psycopg2

from config.settings import DB_CONFIG

SCHEMA = "saude"


def carregar_cid10():
    """Baixa e insere a tabela CID-10 no banco."""
    print("=" * 50)
    print("📋 Carregando CID-10 (Classificação de Doenças)")
    print("=" * 50)
    
    url_cid = "https://raw.githubusercontent.com/cleytonferrari/CidDataSus/master/CIDImport/Repositorio/Resources/CID-10-SUBCATEGORIAS.CSV"
    
    try:
        print("\n📡 Baixando dados...")
        df = pd.read_csv(url_cid, encoding='ISO-8859-1', sep=';', usecols=['SUBCAT', 'DESCRICAO'])
        df.columns = ['codigo_original', 'descricao']
        
        # Remove ponto (J18.9 -> J189)
        df['codigo'] = df['codigo_original'].str.replace('.', '', regex=False)
        df = df.drop_duplicates(subset=['codigo'])
        
        print(f"   ✅ {len(df):,} códigos CID-10 baixados")
        
        # Conecta e insere
        print("\n💾 Salvando no banco...")
        conn = psycopg2.connect(**DB_CONFIG)
        
        with conn.cursor() as cur:
            cur.execute(f"""
                DROP TABLE IF EXISTS {SCHEMA}.cid10 CASCADE;
                
                CREATE TABLE {SCHEMA}.cid10 (
                    codigo VARCHAR(10) PRIMARY KEY,
                    codigo_original VARCHAR(10),
                    descricao TEXT
                );
                
                COMMENT ON TABLE {SCHEMA}.cid10 IS 
                    'Classificação Internacional de Doenças (CID-10). Fonte: OMS/DATASUS';
            """)
            conn.commit()
            
            from io import StringIO
            buffer = StringIO()
            df[['codigo', 'codigo_original', 'descricao']].to_csv(
                buffer, index=False, header=False, sep='\t', na_rep='\\N'
            )
            buffer.seek(0)
            
            cur.copy_expert(
                f"COPY {SCHEMA}.cid10 FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '\\N')",
                buffer
            )
            conn.commit()
        
        conn.close()
        
        print(f"   ✅ Tabela {SCHEMA}.cid10 atualizada!")
        print("\n" + "=" * 50)
        
    except Exception as e:
        print(f"❌ Erro: {e}")


if __name__ == "__main__":
    carregar_cid10()
