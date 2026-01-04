import os
from pathlib import Path

# Carrega variáveis do arquivo .env (se existir)
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")
except ImportError:
    pass  # dotenv não instalado, usa apenas variáveis de ambiente

# ==============================================================================
# BANCO DE DADOS
# ==============================================================================
# Lê variáveis de ambiente (definidas no arquivo .env ou no sistema)
# Copie .env.example para .env e preencha com suas credenciais
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "pesquisa")
DB_USER = os.getenv("DB_USER")  # Obrigatório - configure no .env
DB_PASS = os.getenv("DB_PASS")  # Obrigatório - configure no .env
DB_PORT = os.getenv("DB_PORT", "5432")

# Dicionário de configuração para psycopg2
DB_CONFIG = {
    "host": DB_HOST,
    "database": DB_NAME,
    "user": DB_USER,
    "password": DB_PASS,
    "port": DB_PORT
}

# ==============================================================================
# CAMINHOS
# ==============================================================================
# Raiz do projeto (3 níveis acima deste arquivo: config/ -> ingestao_dados/)
PROJECT_ROOT = Path(__file__).parent.parent
CACHE_DIR = PROJECT_ROOT / "cache"

# ==============================================================================
# CONFIGURAÇÃO DOS ETLs (MODULAR)
# ==============================================================================
# Configuração padrão e específica por fonte
ETL_CONFIG = {
    # Padrão (fallback se não houver específico)
    "padrao": {
        "ufs": ["SP"],
    },
    
    # Censo 2022 - Endereços (CNEFE)
    "cnefe": {
        "ufs": ["SP"], 
        "ativo": True
    },
    
    # Internações (SIH-SUS)
    "internacoes": {
        "ufs": ["SP"],
        "ano_inicio": 2023,
        "ano_fim": 2025
    },
    
    # Estabelecimentos (CNES)
    "estabelecimentos": {
        "ufs": ["SP"],
        # Período de carga (histórico)
        "ano_inicio": 2023,
        "mes_inicio": 1,
        "ano_fim": 2025,
        "mes_fim": 11
    },
    
    # Censo Renda (Setores Censitários)
    "censo_renda": {
        "ufs": ["SP"]
    }
}
