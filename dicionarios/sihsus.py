"""
Dicionário de dados das colunas do SIHSUS (Sistema de Informações Hospitalares).
Baseado na documentação oficial do DATASUS.

Este arquivo contém:
- Mapeamento de nomes originais DATASUS -> nomes intuitivos
- Descrições detalhadas de cada coluna
- Tipos de dados

O nome_intuitivo é usado na tabela, e o nome_original é guardado para referência.
"""

# Mapeamento: nome_original_datasus -> informações completas
# O campo "nome_intuitivo" é o que será usado na tabela do banco
DICIONARIO_SIHSUS = {
    # === IDENTIFICAÇÃO ===
    "uf": {
        "nome_intuitivo": "uf",
        "descricao": "Unidade Federativa (sigla do estado)",
        "tipo": "CHAR(2)",
        "exemplo": "SP, RJ, MG"
    },
    "ano_cmpt": {
        "nome_intuitivo": "ano_competencia",
        "descricao": "Ano de competência da AIH (processamento)",
        "tipo": "INTEGER",
        "exemplo": "2023"
    },
    "mes_cmpt": {
        "nome_intuitivo": "mes_competencia",
        "descricao": "Mês de competência da AIH (processamento)",
        "tipo": "INTEGER",
        "exemplo": "1 a 12"
    },
    "n_aih": {
        "nome_intuitivo": "numero_aih",
        "descricao": "Número da Autorização de Internação Hospitalar",
        "tipo": "TEXT",
        "exemplo": "3523101917698"
    },
    "ident": {
        "nome_intuitivo": "tipo_aih",
        "descricao": "Tipo de AIH: 1=Normal, 5=Longa Permanência",
        "tipo": "TEXT",
        "exemplo": "1, 5"
    },
    "cnes": {
        "nome_intuitivo": "codigo_estabelecimento",
        "descricao": "Código do estabelecimento de saúde (CNES)",
        "tipo": "TEXT",
        "exemplo": "2077485"
    },
    "munic_res": {
        "nome_intuitivo": "municipio_residencia",
        "descricao": "Código IBGE do município de residência do paciente",
        "tipo": "TEXT",
        "exemplo": "355030 (São Paulo)"
    },
    "munic_mov": {
        "nome_intuitivo": "municipio_internacao",
        "descricao": "Código IBGE do município onde ocorreu a internação",
        "tipo": "TEXT",
        "exemplo": "355030 (São Paulo)"
    },
    
    # === DADOS DO PACIENTE ===
    "nasc": {
        "nome_intuitivo": "data_nascimento",
        "descricao": "Data de nascimento do paciente",
        "tipo": "DATE",
        "exemplo": "1985-03-15"
    },
    "sexo": {
        "nome_intuitivo": "sexo",
        "descricao": "Sexo do paciente: 1=Masculino, 3=Feminino",
        "tipo": "CHAR(1)",
        "exemplo": "1, 3"
    },
    "idade": {
        "nome_intuitivo": "idade",
        "descricao": "Idade do paciente na internação",
        "tipo": "INTEGER",
        "exemplo": "45"
    },
    "cod_idade": {
        "nome_intuitivo": "unidade_idade",
        "descricao": "Unidade da idade: 2=Dias, 3=Meses, 4=Anos",
        "tipo": "CHAR(1)",
        "exemplo": "4 (anos)"
    },
    "raca_cor": {
        "nome_intuitivo": "raca_cor",
        "descricao": "Raça/cor: 01=Branca, 02=Preta, 03=Parda, 04=Amarela, 05=Indígena",
        "tipo": "TEXT",
        "exemplo": "01, 02, 03, 04, 05"
    },
    "etnia": {
        "nome_intuitivo": "etnia_indigena",
        "descricao": "Código da etnia indígena (se aplicável)",
        "tipo": "TEXT",
        "exemplo": "Código IBGE etnia"
    },
    "nacional": {
        "nome_intuitivo": "nacionalidade",
        "descricao": "Código do país de nacionalidade",
        "tipo": "TEXT",
        "exemplo": "010 (Brasil)"
    },
    "cep": {
        "nome_intuitivo": "cep_paciente",
        "descricao": "CEP de residência do paciente",
        "tipo": "TEXT",
        "exemplo": "01310100"
    },
    
    # === INTERNAÇÃO ===
    "dt_inter": {
        "nome_intuitivo": "data_internacao",
        "descricao": "Data de internação do paciente",
        "tipo": "DATE",
        "exemplo": "2023-01-15"
    },
    "dt_saida": {
        "nome_intuitivo": "data_saida",
        "descricao": "Data de saída (alta ou óbito)",
        "tipo": "DATE",
        "exemplo": "2023-01-20"
    },
    "dias_perm": {
        "nome_intuitivo": "dias_permanencia",
        "descricao": "Dias de permanência na internação",
        "tipo": "INTEGER",
        "exemplo": "5"
    },
    "qt_diarias": {
        "nome_intuitivo": "quantidade_diarias",
        "descricao": "Quantidade de diárias pagas pelo SUS",
        "tipo": "INTEGER",
        "exemplo": "5"
    },
    "car_int": {
        "nome_intuitivo": "carater_internacao",
        "descricao": "Caráter da internação: 01=Eletiva, 02=Urgência, 03=Acidente, etc.",
        "tipo": "CHAR(2)",
        "exemplo": "01 (Eletiva), 02 (Urgência)"
    },
    
    # === DIAGNÓSTICOS (CID-10) ===
    "diag_princ": {
        "nome_intuitivo": "diagnostico_principal",
        "descricao": "Diagnóstico principal da internação (código CID-10)",
        "tipo": "TEXT",
        "exemplo": "I21 (Infarto agudo do miocárdio)"
    },
    "diag_secun": {
        "nome_intuitivo": "diagnostico_secundario",
        "descricao": "Diagnóstico secundário (código CID-10)",
        "tipo": "TEXT",
        "exemplo": "I10 (Hipertensão essencial)"
    },
    "diagsec1": {"nome_intuitivo": "diagnostico_sec_1", "descricao": "Diagnóstico secundário 1 (CID-10)", "tipo": "TEXT"},
    "diagsec2": {"nome_intuitivo": "diagnostico_sec_2", "descricao": "Diagnóstico secundário 2 (CID-10)", "tipo": "TEXT"},
    "diagsec3": {"nome_intuitivo": "diagnostico_sec_3", "descricao": "Diagnóstico secundário 3 (CID-10)", "tipo": "TEXT"},
    "diagsec4": {"nome_intuitivo": "diagnostico_sec_4", "descricao": "Diagnóstico secundário 4 (CID-10)", "tipo": "TEXT"},
    "diagsec5": {"nome_intuitivo": "diagnostico_sec_5", "descricao": "Diagnóstico secundário 5 (CID-10)", "tipo": "TEXT"},
    "diagsec6": {"nome_intuitivo": "diagnostico_sec_6", "descricao": "Diagnóstico secundário 6 (CID-10)", "tipo": "TEXT"},
    "diagsec7": {"nome_intuitivo": "diagnostico_sec_7", "descricao": "Diagnóstico secundário 7 (CID-10)", "tipo": "TEXT"},
    "diagsec8": {"nome_intuitivo": "diagnostico_sec_8", "descricao": "Diagnóstico secundário 8 (CID-10)", "tipo": "TEXT"},
    "diagsec9": {"nome_intuitivo": "diagnostico_sec_9", "descricao": "Diagnóstico secundário 9 (CID-10)", "tipo": "TEXT"},
    "cid_asso": {
        "nome_intuitivo": "cid_causa_associada",
        "descricao": "CID associado ou causa externa do evento",
        "tipo": "TEXT",
        "exemplo": "W19 (Queda sem especificação)"
    },
    "cid_morte": {
        "nome_intuitivo": "cid_causa_morte",
        "descricao": "CID da causa do óbito (quando houve morte)",
        "tipo": "TEXT",
        "exemplo": "I21.9"
    },
    "cid_notif": {
        "nome_intuitivo": "cid_notificacao",
        "descricao": "CID de notificação compulsória",
        "tipo": "TEXT",
        "exemplo": "A90 (Dengue)"
    },
    
    # === PROCEDIMENTOS ===
    "proc_solic": {
        "nome_intuitivo": "procedimento_solicitado",
        "descricao": "Código do procedimento solicitado (tabela SIGTAP)",
        "tipo": "TEXT",
        "exemplo": "0303010142"
    },
    "proc_rea": {
        "nome_intuitivo": "procedimento_realizado",
        "descricao": "Código do procedimento efetivamente realizado (SIGTAP)",
        "tipo": "TEXT",
        "exemplo": "0303010142"
    },
    
    # === VALORES FINANCEIROS ===
    "val_sh": {
        "nome_intuitivo": "valor_servicos_hospitalares",
        "descricao": "Valor pago por serviços hospitalares (R$)",
        "tipo": "DECIMAL",
        "exemplo": "1500.00"
    },
    "val_sp": {
        "nome_intuitivo": "valor_servicos_profissionais",
        "descricao": "Valor pago por serviços profissionais médicos (R$)",
        "tipo": "DECIMAL",
        "exemplo": "500.00"
    },
    "val_sadt": {
        "nome_intuitivo": "valor_exames_terapias",
        "descricao": "Valor de SADT - exames e terapias (R$)",
        "tipo": "DECIMAL",
        "exemplo": "200.00"
    },
    "val_rn": {
        "nome_intuitivo": "valor_recem_nascido",
        "descricao": "Valor referente a recém-nascido (R$)",
        "tipo": "DECIMAL",
        "exemplo": "0.00"
    },
    "val_acomp": {
        "nome_intuitivo": "valor_acompanhante",
        "descricao": "Valor de diárias de acompanhante (R$)",
        "tipo": "DECIMAL",
        "exemplo": "50.00"
    },
    "val_ortp": {
        "nome_intuitivo": "valor_ortese_protese",
        "descricao": "Valor de órteses e próteses (R$)",
        "tipo": "DECIMAL",
        "exemplo": "1000.00"
    },
    "val_sangue": {
        "nome_intuitivo": "valor_sangue",
        "descricao": "Valor de sangue e hemoderivados (R$)",
        "tipo": "DECIMAL",
        "exemplo": "300.00"
    },
    "val_tot": {
        "nome_intuitivo": "valor_total",
        "descricao": "Valor total pago pela AIH (R$)",
        "tipo": "DECIMAL",
        "exemplo": "3550.00"
    },
    "val_uti": {
        "nome_intuitivo": "valor_uti",
        "descricao": "Valor de diárias de UTI (R$)",
        "tipo": "DECIMAL",
        "exemplo": "2000.00"
    },
    "val_uci": {
        "nome_intuitivo": "valor_uci",
        "descricao": "Valor de UCI - Unidade de Cuidados Intermediários (R$)",
        "tipo": "DECIMAL",
        "exemplo": "800.00"
    },
    "us_tot": {
        "nome_intuitivo": "pontos_servico",
        "descricao": "Total de pontos/unidades de serviço do procedimento",
        "tipo": "DECIMAL",
        "exemplo": "150.00"
    },
    
    # === UTI ===
    "uti_mes_in": {
        "nome_intuitivo": "dias_uti_intensiva",
        "descricao": "Dias de UTI tipo I (intensiva)",
        "tipo": "INTEGER",
        "exemplo": "3"
    },
    "uti_mes_an": {
        "nome_intuitivo": "dias_uti_neonatal",
        "descricao": "Dias de UTI tipo II (neonatal)",
        "tipo": "INTEGER",
        "exemplo": "0"
    },
    "uti_mes_al": {
        "nome_intuitivo": "dias_uti_alto_risco",
        "descricao": "Dias de UTI tipo III (alto risco)",
        "tipo": "INTEGER",
        "exemplo": "0"
    },
    "uti_mes_to": {
        "nome_intuitivo": "dias_uti_total",
        "descricao": "Total de dias em UTI (todas as categorias)",
        "tipo": "INTEGER",
        "exemplo": "3"
    },
    "marca_uti": {
        "nome_intuitivo": "marcador_uti",
        "descricao": "Indicador de uso de UTI",
        "tipo": "CHAR(2)",
        "exemplo": "01=Sim, 00=Não"
    },
    "marca_uci": {
        "nome_intuitivo": "marcador_uci",
        "descricao": "Indicador de uso de UCI",
        "tipo": "CHAR(2)",
        "exemplo": "01=Sim, 00=Não"
    },
    
    # === DESFECHO ===
    "morte": {
        "nome_intuitivo": "obito",
        "descricao": "Indicador de óbito durante internação: 0=Não, 1=Sim",
        "tipo": "CHAR(1)",
        "exemplo": "0 (Alta), 1 (Óbito)"
    },
    "cobranca": {
        "nome_intuitivo": "motivo_cobranca",
        "descricao": "Código do motivo de cobrança da AIH",
        "tipo": "CHAR(2)",
        "exemplo": "01"
    },
    
    # === ESTABELECIMENTO ===
    "natureza": {
        "nome_intuitivo": "natureza_estabelecimento",
        "descricao": "Natureza jurídica do estabelecimento de saúde",
        "tipo": "TEXT",
        "exemplo": "Código de natureza"
    },
    "nat_jur": {
        "nome_intuitivo": "natureza_juridica",
        "descricao": "Natureza jurídica detalhada do estabelecimento",
        "tipo": "TEXT",
        "exemplo": "1023 (Autarquia Federal)"
    },
    "gestao": {
        "nome_intuitivo": "tipo_gestao",
        "descricao": "Tipo de gestão do estabelecimento: E=Estadual, M=Municipal",
        "tipo": "CHAR(2)",
        "exemplo": "E, M"
    },
    "complex": {
        "nome_intuitivo": "complexidade",
        "descricao": "Nível de complexidade: 01=Básica, 02=Média, 03=Alta",
        "tipo": "CHAR(2)",
        "exemplo": "02 (Média complexidade)"
    },
    "financ": {
        "nome_intuitivo": "tipo_financiamento",
        "descricao": "Tipo de financiamento da AIH",
        "tipo": "CHAR(2)",
        "exemplo": "04 (FAEC), 06 (MAC)"
    },
    
    # === CONTROLE ===
    "arquivo_origem": {
        "nome_intuitivo": "arquivo_origem",
        "descricao": "Nome do arquivo fonte baixado do DATASUS",
        "tipo": "TEXT",
        "exemplo": "RDSP2301"
    },
    "data_carga": {
        "nome_intuitivo": "data_carga",
        "descricao": "Data e hora de inserção no banco local",
        "tipo": "TIMESTAMP",
        "exemplo": "2024-01-01 10:30:00"
    },
    "sequencia": {
        "nome_intuitivo": "sequencia_registro",
        "descricao": "Número de sequência do registro no arquivo",
        "tipo": "INTEGER",
        "exemplo": "1"
    },
    "remessa": {
        "nome_intuitivo": "codigo_remessa",
        "descricao": "Código da remessa de dados ao DATASUS",
        "tipo": "TEXT",
        "exemplo": "202301"
    },
    
    # === OUTROS ===
    "diar_acom": {
        "nome_intuitivo": "diarias_acompanhante",
        "descricao": "Quantidade de diárias de acompanhante",
        "tipo": "INTEGER",
        "exemplo": "5"
    },
    "espec": {
        "nome_intuitivo": "especialidade_leito",
        "descricao": "Código da especialidade do leito",
        "tipo": "TEXT",
        "exemplo": "01 (Cirurgia geral)"
    },
    "cgc_hosp": {
        "nome_intuitivo": "cnpj_hospital",
        "descricao": "CNPJ do hospital",
        "tipo": "TEXT",
        "exemplo": "12345678000190"
    },
    "cnpj_mant": {
        "nome_intuitivo": "cnpj_mantenedora",
        "descricao": "CNPJ da entidade mantenedora do hospital",
        "tipo": "TEXT",
        "exemplo": "12345678000190"
    },
    "infehosp": {
        "nome_intuitivo": "infeccao_hospitalar",
        "descricao": "Indicador de infecção hospitalar: 0=Não, 1=Sim",
        "tipo": "CHAR(1)",
        "exemplo": "0, 1"
    },
    "ind_vdrl": {
        "nome_intuitivo": "indicador_vdrl",
        "descricao": "Resultado do exame VDRL (sífilis): 0=Neg, 1=Pos, 2=Não realizado",
        "tipo": "CHAR(1)",
        "exemplo": "0, 1, 2"
    },
    "gestrisco": {
        "nome_intuitivo": "gestacao_risco",
        "descricao": "Indicador de gestação de alto risco",
        "tipo": "TEXT",
        "exemplo": "0=Não, 1=Sim"
    },
    "num_filhos": {
        "nome_intuitivo": "numero_filhos",
        "descricao": "Número de filhos (em partos)",
        "tipo": "TEXT",
        "exemplo": "2"
    },
    "instru": {
        "nome_intuitivo": "escolaridade",
        "descricao": "Grau de instrução/escolaridade do paciente",
        "tipo": "TEXT",
        "exemplo": "1 a 5"
    },
    "contracep1": {
        "nome_intuitivo": "metodo_contraceptivo_1",
        "descricao": "Primeiro método contraceptivo prescrito",
        "tipo": "TEXT",
        "exemplo": "Código método"
    },
    "contracep2": {
        "nome_intuitivo": "metodo_contraceptivo_2",
        "descricao": "Segundo método contraceptivo prescrito",
        "tipo": "TEXT",
        "exemplo": "Código método"
    },
    "cbor": {
        "nome_intuitivo": "ocupacao_paciente",
        "descricao": "Código CBO da ocupação/profissão do paciente",
        "tipo": "TEXT",
        "exemplo": "517420 (Porteiro)"
    },
    "cnaer": {
        "nome_intuitivo": "atividade_economica",
        "descricao": "Código CNAE da atividade econômica do paciente",
        "tipo": "TEXT",
        "exemplo": "0111301"
    },
    "vincprev": {
        "nome_intuitivo": "vinculo_previdenciario",
        "descricao": "Tipo de vínculo previdenciário do paciente",
        "tipo": "TEXT",
        "exemplo": "1 (CLT)"
    },
    "tot_pt_sp": {
        "nome_intuitivo": "total_pontos_sp",
        "descricao": "Total de pontos de serviço profissional",
        "tipo": "INTEGER",
        "exemplo": "100"
    },
    "uf_zi": {
        "nome_intuitivo": "uf_estabelecimento",
        "descricao": "UF do estabelecimento de saúde",
        "tipo": "TEXT",
        "exemplo": "SP"
    },
    "rubrica": {
        "nome_intuitivo": "rubrica_financeira",
        "descricao": "Código da rubrica financeira",
        "tipo": "TEXT",
        "exemplo": "Código rubrica"
    },
    "num_proc": {
        "nome_intuitivo": "numero_procedimento",
        "descricao": "Número sequencial do procedimento",
        "tipo": "TEXT",
        "exemplo": "1"
    },
    "cpf_aut": {
        "nome_intuitivo": "cpf_autorizador",
        "descricao": "CPF do profissional autorizador da AIH",
        "tipo": "TEXT",
        "exemplo": "12345678900"
    },
    "homonimo": {
        "nome_intuitivo": "homonimo",
        "descricao": "Indicador de homônimo",
        "tipo": "TEXT",
        "exemplo": "S/N"
    },
    "insc_pn": {
        "nome_intuitivo": "inscricao_pre_natal",
        "descricao": "Número de inscrição no pré-natal",
        "tipo": "TEXT",
        "exemplo": "Código inscrição"
    },
    "seq_aih5": {
        "nome_intuitivo": "sequencia_aih_longa",
        "descricao": "Sequência de AIH de longa permanência",
        "tipo": "TEXT",
        "exemplo": "1, 2, 3..."
    },
    "gestor_cod": {
        "nome_intuitivo": "codigo_gestor",
        "descricao": "Código do gestor responsável",
        "tipo": "TEXT",
        "exemplo": "Código gestor"
    },
    "gestor_tp": {
        "nome_intuitivo": "tipo_gestor",
        "descricao": "Tipo de gestor",
        "tipo": "TEXT",
        "exemplo": "E=Estadual, M=Municipal"
    },
    "gestor_cpf": {
        "nome_intuitivo": "cpf_gestor",
        "descricao": "CPF do gestor",
        "tipo": "TEXT",
        "exemplo": "12345678900"
    },
    "gestor_dt": {
        "nome_intuitivo": "data_autorizacao_gestor",
        "descricao": "Data de autorização pelo gestor",
        "tipo": "TEXT",
        "exemplo": "2023-01-15"
    },
    "faec_tp": {
        "nome_intuitivo": "tipo_faec",
        "descricao": "Tipo de Fundo de Ações Estratégicas e Compensação",
        "tipo": "TEXT",
        "exemplo": "Código FAEC"
    },
}


def obter_nome_intuitivo(nome_original):
    """Retorna o nome intuitivo para uma coluna, ou o próprio nome se não mapeado."""
    nome_lower = nome_original.lower()
    if nome_lower in DICIONARIO_SIHSUS:
        return DICIONARIO_SIHSUS[nome_lower].get("nome_intuitivo", nome_lower)
    return nome_lower


def obter_descricao(coluna_original):
    """Retorna a descrição de uma coluna."""
    col_lower = coluna_original.lower()
    if col_lower in DICIONARIO_SIHSUS:
        info = DICIONARIO_SIHSUS[col_lower]
        nome_original = col_lower
        return f"{info.get('descricao', '')} [DATASUS: {nome_original}]"
    return f"Coluna do SIHSUS [DATASUS: {coluna_original}]"


def obter_mapeamento_colunas():
    """Retorna dicionário de mapeamento nome_original -> nome_intuitivo."""
    return {k: v["nome_intuitivo"] for k, v in DICIONARIO_SIHSUS.items()}


def obter_dicionario_completo():
    """Retorna o dicionário completo para criar tabela de metadados."""
    return DICIONARIO_SIHSUS
