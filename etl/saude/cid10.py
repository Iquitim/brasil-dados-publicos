"""
Script para carregar/atualizar tabela CID-10 no schema saude.
Classificação Internacional de Doenças da OMS.

Uso:
    source venv/bin/activate
    python carregar_cid10.py
"""

import pandas as pd
import psycopg2

from config.settings import DB_CONFIG, PROJECT_ROOT

SCHEMA = "saude"


def carregar_cid10(tipo_saida="postgres"):
    """
    Baixa e insere a tabela CID-10.
    
    Args:
        tipo_saida (str): 'postgres' para salvar no banco, 'csv' para salvar arquivo local.
    """
    print("=" * 50)
    print("📋 Carregando CID-10 (Classificação de Doenças)")
    print("=" * 50)
    
    url_cid = "https://raw.githubusercontent.com/cleytonferrari/CidDataSus/master/CIDImport/Repositorio/Resources/CID-10-SUBCATEGORIAS.CSV"
    
    try:
        print("\n📡 Baixando dados...")
        df = pd.read_csv(url_cid, encoding='ISO-8859-1', sep=';', usecols=['SUBCAT', 'DESCRICAO'])
        df.columns = ['codigo_original', 'descricao']
        
        # Remove ponto (J18.9 -> J189)
        import numpy as np
        df['codigo'] = df['codigo_original'].str.replace('.', '', regex=False)
        df = df.drop_duplicates(subset=['codigo'])
        
        print(f"   ✅ {len(df):,} códigos CID-10 baixados")
        
        if tipo_saida == "postgres":
            # Conecta e insere
            print("\n💾 Salvando no banco...")
            conn = psycopg2.connect(**DB_CONFIG)
            
            with conn.cursor() as cur:
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {SCHEMA}.cid10 (
                        codigo VARCHAR(10) PRIMARY KEY,
                        codigo_original VARCHAR(10),
                        descricao TEXT
                    );
                    
                    TRUNCATE TABLE {SCHEMA}.cid10;
                    
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
            
        elif tipo_saida == "csv":
            output_dir = PROJECT_ROOT / "output"
            output_dir.mkdir(exist_ok=True)
            output_file = output_dir / "cid10.csv"
            
            print(f"\n💾 Salvando CSV em: {output_file}")
            df[['codigo', 'codigo_original', 'descricao']].to_csv(output_file, index=False, sep=';', encoding='utf-8')
            print(f"   ✅ Arquivo salvo com sucesso!")
            
        print("\n" + "=" * 50)
        
    except Exception as e:
        print(f"❌ Erro: {e}")


if __name__ == "__main__":
    carregar_cid10()
