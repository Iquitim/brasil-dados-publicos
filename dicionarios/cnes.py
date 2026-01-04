"""
Dicionário de dados das colunas do CNES (Cadastro Nacional de Estabelecimentos de Saúde).
Baseado na documentação oficial do DATASUS.

Este arquivo contém:
- Mapeamento de nomes originais DATASUS -> nomes intuitivos
- Descrições detalhadas de cada coluna
- Tipos de dados

O nome_intuitivo é usado na tabela, e o nome_original é guardado para referência.
"""

# Mapeamento: nome_original_datasus -> informações completas
# O campo "nome_intuitivo" é o que será usado na tabela do banco
DICIONARIO_CNES = {
    # === IDENTIFICAÇÃO DO ESTABELECIMENTO ===
    "cnes": {
        "nome_intuitivo": "codigo_cnes",
        "descricao": "Código do Cadastro Nacional de Estabelecimentos de Saúde",
        "tipo": "TEXT",
        "exemplo": "2077485"
    },
    "codufmun": {
        "nome_intuitivo": "codigo_municipio",
        "descricao": "Código IBGE do município (UF + código municipal sem DV)",
        "tipo": "TEXT",
        "exemplo": "355030 (São Paulo)"
    },
    "cod_cep": {
        "nome_intuitivo": "cep",
        "descricao": "CEP do endereço do estabelecimento",
        "tipo": "TEXT",
        "exemplo": "01310100"
    },
    "cpf_cnpj": {
        "nome_intuitivo": "cpf_cnpj",
        "descricao": "CPF (pessoa física) ou CNPJ (pessoa jurídica) do estabelecimento",
        "tipo": "TEXT",
        "exemplo": "12345678000190"
    },
    "pf_pj": {
        "nome_intuitivo": "tipo_pessoa",
        "descricao": "Tipo de pessoa: 1=Física, 3=Jurídica",
        "tipo": "CHAR(1)",
        "exemplo": "1, 3"
    },
    "cnpj_man": {
        "nome_intuitivo": "cnpj_mantenedora",
        "descricao": "CNPJ da entidade mantenedora do estabelecimento",
        "tipo": "TEXT",
        "exemplo": "12345678000190"
    },
    
    # === CARACTERÍSTICAS DO ESTABELECIMENTO ===
    "tp_unid": {
        "nome_intuitivo": "tipo_unidade",
        "descricao": "Código do tipo de unidade de saúde (hospital, UBS, clínica, etc.)",
        "tipo": "TEXT",
        "exemplo": "05=Hospital, 01=Posto de Saúde"
    },
    "natureza": {
        "nome_intuitivo": "natureza_organizacao",
        "descricao": "Natureza da organização do estabelecimento",
        "tipo": "TEXT",
        "exemplo": "Código natureza"
    },
    "nat_jur": {
        "nome_intuitivo": "natureza_juridica",
        "descricao": "Natureza jurídica conforme tabela do IBGE",
        "tipo": "TEXT",
        "exemplo": "1023 (Autarquia Federal)"
    },
    "esfera_a": {
        "nome_intuitivo": "esfera_administrativa",
        "descricao": "Esfera administrativa: 01=Federal, 02=Estadual, 03=Municipal, 04=Privada",
        "tipo": "TEXT",
        "exemplo": "01, 02, 03, 04"
    },
    "tpgestao": {
        "nome_intuitivo": "tipo_gestao",
        "descricao": "Tipo de gestão: E=Estadual, M=Municipal, D=Dupla, S=Sem gestão",
        "tipo": "CHAR(1)",
        "exemplo": "E, M, D, S"
    },
    "tp_prest": {
        "nome_intuitivo": "tipo_prestador",
        "descricao": "Tipo de prestador de serviços de saúde",
        "tipo": "TEXT",
        "exemplo": "Código prestador"
    },
    "niv_dep": {
        "nome_intuitivo": "nivel_dependencia",
        "descricao": "Nível de dependência: 1=Individual, 3=Mantido",
        "tipo": "CHAR(1)",
        "exemplo": "1, 3"
    },
    "niv_hier": {
        "nome_intuitivo": "nivel_hierarquia",
        "descricao": "Nível de hierarquia/complexidade do estabelecimento",
        "tipo": "TEXT",
        "exemplo": "Código hierarquia"
    },
    "clientel": {
        "nome_intuitivo": "clientela",
        "descricao": "Tipo de clientela atendida pelo estabelecimento",
        "tipo": "TEXT",
        "exemplo": "Código clientela"
    },
    "atividad": {
        "nome_intuitivo": "atividade_ensino",
        "descricao": "Código de atividade de ensino/pesquisa",
        "tipo": "TEXT",
        "exemplo": "Código atividade"
    },
    "turno_at": {
        "nome_intuitivo": "turno_atendimento",
        "descricao": "Turno de atendimento do estabelecimento",
        "tipo": "TEXT",
        "exemplo": "Código turno"
    },
    
    # === VÍNCULO E ATENDIMENTO ===
    "vinc_sus": {
        "nome_intuitivo": "vinculo_sus",
        "descricao": "Indicador de vínculo com o SUS: S=Sim, N=Não",
        "tipo": "CHAR(1)",
        "exemplo": "S, N"
    },
    "atendamb": {
        "nome_intuitivo": "atendimento_ambulatorial",
        "descricao": "Indicador de atendimento ambulatorial",
        "tipo": "TEXT",
        "exemplo": "S/N"
    },
    "atendhos": {
        "nome_intuitivo": "atendimento_hospitalar",
        "descricao": "Indicador de atendimento hospitalar/internação",
        "tipo": "TEXT",
        "exemplo": "S/N"
    },
    "atend_pr": {
        "nome_intuitivo": "atend_pronto_socorro",
        "descricao": "Indicador de atendimento de pronto-socorro",
        "tipo": "TEXT",
        "exemplo": "S/N"
    },
    "urgemerg": {
        "nome_intuitivo": "urgencia_emergencia",
        "descricao": "Indicador de atendimento de urgência/emergência",
        "tipo": "TEXT",
        "exemplo": "S/N"
    },
    "nivate_a": {
        "nome_intuitivo": "nivel_atencao_ambulatorial",
        "descricao": "Nível de atenção ambulatorial",
        "tipo": "TEXT",
        "exemplo": "Código nível"
    },
    "nivate_h": {
        "nome_intuitivo": "nivel_atencao_hospitalar",
        "descricao": "Nível de atenção hospitalar",
        "tipo": "TEXT",
        "exemplo": "Código nível"
    },
    
    # === ESTRUTURA FÍSICA ===
    "leithosp": {
        "nome_intuitivo": "leitos_hospital",
        "descricao": "Quantidade total de leitos hospitalares",
        "tipo": "INTEGER",
        "exemplo": "100"
    },
    "centrcir": {
        "nome_intuitivo": "centro_cirurgico",
        "descricao": "Indicador de centro cirúrgico",
        "tipo": "TEXT",
        "exemplo": "S/N"
    },
    "centrobs": {
        "nome_intuitivo": "centro_obstetrico",
        "descricao": "Indicador de centro obstétrico",
        "tipo": "TEXT",
        "exemplo": "S/N"
    },
    "centrneo": {
        "nome_intuitivo": "centro_neonatal",
        "descricao": "Indicador de centro neonatal",
        "tipo": "TEXT",
        "exemplo": "S/N"
    },
    
    # === LEITOS POR TIPO ===
    "qtleit05": {
        "nome_intuitivo": "leitos_cirurgicos",
        "descricao": "Quantidade de leitos cirúrgicos",
        "tipo": "INTEGER",
        "exemplo": "20"
    },
    "qtleit06": {
        "nome_intuitivo": "leitos_clinicos",
        "descricao": "Quantidade de leitos clínicos",
        "tipo": "INTEGER",
        "exemplo": "30"
    },
    "qtleit07": {
        "nome_intuitivo": "leitos_complementares",
        "descricao": "Quantidade de leitos complementares",
        "tipo": "INTEGER",
        "exemplo": "10"
    },
    "qtleit08": {
        "nome_intuitivo": "leitos_obstetricos",
        "descricao": "Quantidade de leitos obstétricos",
        "tipo": "INTEGER",
        "exemplo": "15"
    },
    "qtleit09": {
        "nome_intuitivo": "leitos_outras_especialidades",
        "descricao": "Quantidade de leitos de outras especialidades",
        "tipo": "INTEGER",
        "exemplo": "5"
    },
    "qtleit19": {
        "nome_intuitivo": "leitos_pediatricos",
        "descricao": "Quantidade de leitos pediátricos",
        "tipo": "INTEGER",
        "exemplo": "25"
    },
    "qtleit20": {
        "nome_intuitivo": "leitos_hospital_dia",
        "descricao": "Quantidade de leitos de hospital-dia",
        "tipo": "INTEGER",
        "exemplo": "10"
    },
    "qtleit21": {
        "nome_intuitivo": "leitos_repouso",
        "descricao": "Quantidade de leitos de repouso/observação",
        "tipo": "INTEGER",
        "exemplo": "8"
    },
    "qtleit22": {
        "nome_intuitivo": "leitos_urgencia",
        "descricao": "Quantidade de leitos de urgência",
        "tipo": "INTEGER",
        "exemplo": "12"
    },
    "qtleit23": {
        "nome_intuitivo": "leitos_isolamento",
        "descricao": "Quantidade de leitos de isolamento",
        "tipo": "INTEGER",
        "exemplo": "4"
    },
    "qtleit32": {
        "nome_intuitivo": "leitos_uti_adulto_i",
        "descricao": "Quantidade de leitos UTI adulto tipo I",
        "tipo": "INTEGER",
        "exemplo": "10"
    },
    "qtleit34": {
        "nome_intuitivo": "leitos_uti_adulto_ii",
        "descricao": "Quantidade de leitos UTI adulto tipo II",
        "tipo": "INTEGER",
        "exemplo": "8"
    },
    "qtleit38": {
        "nome_intuitivo": "leitos_uti_neonatal",
        "descricao": "Quantidade de leitos UTI neonatal",
        "tipo": "INTEGER",
        "exemplo": "6"
    },
    "qtleit39": {
        "nome_intuitivo": "leitos_uti_pediatrica",
        "descricao": "Quantidade de leitos UTI pediátrica",
        "tipo": "INTEGER",
        "exemplo": "4"
    },
    "qtleit40": {
        "nome_intuitivo": "leitos_uti_queimados",
        "descricao": "Quantidade de leitos UTI para queimados",
        "tipo": "INTEGER",
        "exemplo": "2"
    },
    "qtleitp1": {
        "nome_intuitivo": "leitos_psiquiatricos_sus",
        "descricao": "Quantidade de leitos psiquiátricos SUS",
        "tipo": "INTEGER",
        "exemplo": "10"
    },
    "qtleitp2": {
        "nome_intuitivo": "leitos_psiquiatricos_nao_sus",
        "descricao": "Quantidade de leitos psiquiátricos não-SUS",
        "tipo": "INTEGER",
        "exemplo": "5"
    },
    "qtleitp3": {
        "nome_intuitivo": "leitos_psiquiatricos_total",
        "descricao": "Quantidade total de leitos psiquiátricos",
        "tipo": "INTEGER",
        "exemplo": "15"
    },
    
    # === INSTALAÇÕES ===
    "qtinst01": {
        "nome_intuitivo": "salas_cirurgia",
        "descricao": "Quantidade de salas de cirurgia",
        "tipo": "INTEGER",
        "exemplo": "5"
    },
    "qtinst02": {
        "nome_intuitivo": "salas_pequena_cirurgia",
        "descricao": "Quantidade de salas de pequena cirurgia/procedimentos",
        "tipo": "INTEGER",
        "exemplo": "3"
    },
    "qtinst03": {
        "nome_intuitivo": "salas_parto",
        "descricao": "Quantidade de salas de parto normal",
        "tipo": "INTEGER",
        "exemplo": "2"
    },
    "qtinst04": {
        "nome_intuitivo": "salas_curetagem",
        "descricao": "Quantidade de salas de curetagem",
        "tipo": "INTEGER",
        "exemplo": "1"
    },
    "qtinst05": {
        "nome_intuitivo": "salas_pre_parto",
        "descricao": "Quantidade de salas de pré-parto",
        "tipo": "INTEGER",
        "exemplo": "2"
    },
    "qtinst06": {
        "nome_intuitivo": "consultórios_medicos",
        "descricao": "Quantidade de consultórios médicos",
        "tipo": "INTEGER",
        "exemplo": "10"
    },
    "qtinst07": {
        "nome_intuitivo": "consultorios_odontologicos",
        "descricao": "Quantidade de consultórios odontológicos",
        "tipo": "INTEGER",
        "exemplo": "3"
    },
    "qtinst08": {
        "nome_intuitivo": "salas_urgencia",
        "descricao": "Quantidade de salas de urgência/emergência",
        "tipo": "INTEGER",
        "exemplo": "4"
    },
    "qtinst09": {
        "nome_intuitivo": "salas_nebulizacao",
        "descricao": "Quantidade de salas de nebulização/inalação",
        "tipo": "INTEGER",
        "exemplo": "2"
    },
    "qtinst10": {
        "nome_intuitivo": "salas_enfermagem",
        "descricao": "Quantidade de salas de enfermagem",
        "tipo": "INTEGER",
        "exemplo": "5"
    },
    "qtinst11": {
        "nome_intuitivo": "salas_imunizacao",
        "descricao": "Quantidade de salas de imunização/vacinação",
        "tipo": "INTEGER",
        "exemplo": "2"
    },
    "qtinst12": {
        "nome_intuitivo": "salas_curativo",
        "descricao": "Quantidade de salas de curativos",
        "tipo": "INTEGER",
        "exemplo": "2"
    },
    "qtinst13": {
        "nome_intuitivo": "salas_coleta",
        "descricao": "Quantidade de salas de coleta de material",
        "tipo": "INTEGER",
        "exemplo": "2"
    },
    "qtinst14": {
        "nome_intuitivo": "salas_radiologia",
        "descricao": "Quantidade de salas de raio-X/radiologia",
        "tipo": "INTEGER",
        "exemplo": "2"
    },
    
    # === FINANCEIRO ===
    "co_banco": {
        "nome_intuitivo": "codigo_banco",
        "descricao": "Código do banco para pagamentos",
        "tipo": "TEXT",
        "exemplo": "001 (Banco do Brasil)"
    },
    "co_agenc": {
        "nome_intuitivo": "codigo_agencia",
        "descricao": "Código da agência bancária",
        "tipo": "TEXT",
        "exemplo": "1234"
    },
    "c_corren": {
        "nome_intuitivo": "conta_corrente",
        "descricao": "Número da conta corrente",
        "tipo": "TEXT",
        "exemplo": "12345-6"
    },
    "retencao": {
        "nome_intuitivo": "retencao_tributos",
        "descricao": "Indicador de retenção de tributos",
        "tipo": "TEXT",
        "exemplo": "S/N"
    },
    
    # === CONTRATO E ACREDITAÇÃO ===
    "alvara": {
        "nome_intuitivo": "alvara_sanitario",
        "descricao": "Número do alvará sanitário",
        "tipo": "TEXT",
        "exemplo": "Número alvará"
    },
    "dt_exped": {
        "nome_intuitivo": "data_expedicao_alvara",
        "descricao": "Data de expedição do alvará",
        "tipo": "DATE",
        "exemplo": "2023-01-15"
    },
    "orgexped": {
        "nome_intuitivo": "orgao_expedidor_alvara",
        "descricao": "Órgão expedidor do alvará sanitário",
        "tipo": "TEXT",
        "exemplo": "Vigilância Sanitária"
    },
    "av_acred": {
        "nome_intuitivo": "avaliacao_acreditacao",
        "descricao": "Indicador de acreditação hospitalar",
        "tipo": "TEXT",
        "exemplo": "S/N"
    },
    "clasaval": {
        "nome_intuitivo": "classe_acreditacao",
        "descricao": "Classe da acreditação (ONA, JCI, etc.)",
        "tipo": "TEXT",
        "exemplo": "Código classe"
    },
    "dt_acred": {
        "nome_intuitivo": "data_acreditacao",
        "descricao": "Data da acreditação",
        "tipo": "DATE",
        "exemplo": "2022-06-01"
    },
    "av_pnass": {
        "nome_intuitivo": "avaliacao_pnass",
        "descricao": "Indicador do PNASS (Programa Nacional de Avaliação)",
        "tipo": "TEXT",
        "exemplo": "S/N"
    },
    "dt_pnass": {
        "nome_intuitivo": "data_pnass",
        "descricao": "Data da avaliação PNASS",
        "tipo": "DATE",
        "exemplo": "2023-03-01"
    },
    "contrate": {
        "nome_intuitivo": "contrato_estadual",
        "descricao": "Indicador de contrato com gestão estadual",
        "tipo": "TEXT",
        "exemplo": "S/N"
    },
    "contratm": {
        "nome_intuitivo": "contrato_municipal",
        "descricao": "Indicador de contrato com gestão municipal",
        "tipo": "TEXT",
        "exemplo": "S/N"
    },
    
    # === LOCALIZAÇÃO E REGIÃO ===
    "regsaude": {
        "nome_intuitivo": "regiao_saude",
        "descricao": "Código da região de saúde",
        "tipo": "TEXT",
        "exemplo": "35001"
    },
    "micr_reg": {
        "nome_intuitivo": "microrregiao_saude",
        "descricao": "Código da microrregião de saúde",
        "tipo": "TEXT",
        "exemplo": "35001"
    },
    "distrsan": {
        "nome_intuitivo": "distrito_sanitario",
        "descricao": "Código do distrito sanitário",
        "tipo": "TEXT",
        "exemplo": "Código distrito"
    },
    "distradm": {
        "nome_intuitivo": "distrito_administrativo",
        "descricao": "Código do distrito administrativo",
        "tipo": "TEXT",
        "exemplo": "Código distrito"
    },
    
    # === GESTÃO DE RESÍDUOS ===
    "coletres": {
        "nome_intuitivo": "coleta_residuos",
        "descricao": "Indicador de coleta de resíduos de saúde",
        "tipo": "TEXT",
        "exemplo": "S/N"
    },
    "res_biol": {
        "nome_intuitivo": "residuos_biologicos",
        "descricao": "Indicador de tratamento de resíduos biológicos",
        "tipo": "TEXT",
        "exemplo": "S/N"
    },
    "res_quim": {
        "nome_intuitivo": "residuos_quimicos",
        "descricao": "Indicador de tratamento de resíduos químicos",
        "tipo": "TEXT",
        "exemplo": "S/N"
    },
    "res_radi": {
        "nome_intuitivo": "residuos_radioativos",
        "descricao": "Indicador de tratamento de resíduos radioativos",
        "tipo": "TEXT",
        "exemplo": "S/N"
    },
    "res_comu": {
        "nome_intuitivo": "residuos_comuns",
        "descricao": "Indicador de tratamento de resíduos comuns",
        "tipo": "TEXT",
        "exemplo": "S/N"
    },
    
    # === DATAS E CONTROLE ===
    "competen": {
        "nome_intuitivo": "competencia",
        "descricao": "Mês/ano de competência dos dados (AAAAMM)",
        "tipo": "TEXT",
        "exemplo": "202401"
    },
    "dt_atual": {
        "nome_intuitivo": "data_ultima_atualizacao",
        "descricao": "Data da última atualização do cadastro",
        "tipo": "DATE",
        "exemplo": "2024-01-15"
    },
    "dt_puble": {
        "nome_intuitivo": "data_publicacao_estadual",
        "descricao": "Data de publicação pelo gestor estadual",
        "tipo": "DATE",
        "exemplo": "2024-01-20"
    },
    "dt_publm": {
        "nome_intuitivo": "data_publicacao_municipal",
        "descricao": "Data de publicação pelo gestor municipal",
        "tipo": "DATE",
        "exemplo": "2024-01-18"
    },
    "cod_ir": {
        "nome_intuitivo": "codigo_identificador_registro",
        "descricao": "Código identificador do registro",
        "tipo": "TEXT",
        "exemplo": "Código IR"
    },
    
    # === SERVIÇOS ESPECIALIZADOS (SERAP) ===
    "serapoio": {
        "nome_intuitivo": "servico_apoio",
        "descricao": "Indicador de serviços de apoio diagnóstico e terapêutico",
        "tipo": "TEXT",
        "exemplo": "S/N"
    },
    "serap01p": {
        "nome_intuitivo": "servico_01_proprio",
        "descricao": "Serviço especializado 01 próprio",
        "tipo": "TEXT",
        "exemplo": "S/N"
    },
    "serap01t": {
        "nome_intuitivo": "servico_01_terceirizado",
        "descricao": "Serviço especializado 01 terceirizado",
        "tipo": "TEXT",
        "exemplo": "S/N"
    },
    
    # === COMISSÕES ===
    "comissao": {
        "nome_intuitivo": "comissoes",
        "descricao": "Indicador de existência de comissões hospitalares",
        "tipo": "TEXT",
        "exemplo": "S/N"
    },
    "comiss01": {
        "nome_intuitivo": "comissao_etica_medica",
        "descricao": "Comissão de Ética Médica",
        "tipo": "TEXT",
        "exemplo": "S/N"
    },
    "comiss02": {
        "nome_intuitivo": "comissao_etica_enfermagem",
        "descricao": "Comissão de Ética de Enfermagem",
        "tipo": "TEXT",
        "exemplo": "S/N"
    },
    "comiss03": {
        "nome_intuitivo": "comissao_farmacia",
        "descricao": "Comissão de Farmácia e Terapêutica",
        "tipo": "TEXT",
        "exemplo": "S/N"
    },
    "comiss04": {
        "nome_intuitivo": "comissao_ccih",
        "descricao": "CCIH - Comissão de Controle de Infecção Hospitalar",
        "tipo": "TEXT",
        "exemplo": "S/N"
    },
    "comiss05": {
        "nome_intuitivo": "comissao_apropriacao_custos",
        "descricao": "Comissão de Apropriação de Custos",
        "tipo": "TEXT",
        "exemplo": "S/N"
    },
    "comiss06": {
        "nome_intuitivo": "comissao_cipa",
        "descricao": "CIPA - Comissão Interna Prev. Acidentes",
        "tipo": "TEXT",
        "exemplo": "S/N"
    },
    "comiss07": {
        "nome_intuitivo": "comissao_revisao_prontuarios",
        "descricao": "Comissão de Revisão de Prontuários",
        "tipo": "TEXT",
        "exemplo": "S/N"
    },
    "comiss08": {
        "nome_intuitivo": "comissao_revisao_obitos",
        "descricao": "Comissão de Revisão de Óbitos",
        "tipo": "TEXT",
        "exemplo": "S/N"
    },
    "comiss09": {
        "nome_intuitivo": "comissao_analise_documentos",
        "descricao": "Comissão de Análise de Documentos Médicos",
        "tipo": "TEXT",
        "exemplo": "S/N"
    },
    "comiss10": {
        "nome_intuitivo": "comissao_transplante",
        "descricao": "Comissão de Transplante",
        "tipo": "TEXT",
        "exemplo": "S/N"
    },
    "comiss11": {
        "nome_intuitivo": "comissao_humanizacao",
        "descricao": "Comissão de Humanização",
        "tipo": "TEXT",
        "exemplo": "S/N"
    },
    "comiss12": {
        "nome_intuitivo": "comissao_residuos",
        "descricao": "Comissão de Resíduos de Serviços de Saúde",
        "tipo": "TEXT",
        "exemplo": "S/N"
    },
    
    # === GESTÃO PROGRAMÁTICA ===
    "gesprg1e": {
        "nome_intuitivo": "gestao_prog_estadual_1",
        "descricao": "Gestão programática estadual 1",
        "tipo": "TEXT",
        "exemplo": "Código"
    },
    "gesprg1m": {
        "nome_intuitivo": "gestao_prog_municipal_1",
        "descricao": "Gestão programática municipal 1",
        "tipo": "TEXT",
        "exemplo": "Código"
    },
    
    # === CONTROLE ===
    "arquivo_origem": {
        "nome_intuitivo": "arquivo_origem",
        "descricao": "Nome do arquivo fonte baixado do DATASUS",
        "tipo": "TEXT",
        "exemplo": "STSP2401"
    },
    "data_carga": {
        "nome_intuitivo": "data_carga",
        "descricao": "Data e hora de inserção no banco local",
        "tipo": "TIMESTAMP",
        "exemplo": "2024-01-01 10:30:00"
    },
}


def obter_nome_intuitivo(nome_original):
    """Retorna o nome intuitivo para uma coluna, ou o próprio nome se não mapeado."""
    nome_lower = nome_original.lower()
    if nome_lower in DICIONARIO_CNES:
        return DICIONARIO_CNES[nome_lower].get("nome_intuitivo", nome_lower)
    return nome_lower


def obter_descricao(coluna_original):
    """Retorna a descrição de uma coluna."""
    col_lower = coluna_original.lower()
    if col_lower in DICIONARIO_CNES:
        info = DICIONARIO_CNES[col_lower]
        nome_original = col_lower
        return f"{info.get('descricao', '')} [DATASUS: {nome_original.upper()}]"
    return f"Coluna do CNES [DATASUS: {coluna_original}]"


def obter_mapeamento_colunas():
    """Retorna dicionário de mapeamento nome_original -> nome_intuitivo."""
    return {k: v["nome_intuitivo"] for k, v in DICIONARIO_CNES.items()}


def obter_dicionario_completo():
    """Retorna o dicionário completo para criar tabela de metadados."""
    return DICIONARIO_CNES
