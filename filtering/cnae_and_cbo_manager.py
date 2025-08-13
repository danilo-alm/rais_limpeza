import os
import numpy as np
import pandas as pd


class CnaeAndCboManager:
    
    @staticmethod
    def generate_dict_from_spreadsheet(file_path: str, one_column: bool = False,
                                      sheet_name: str = None, csv: bool = False, **read_csv_args):
        _, ext = os.path.splitext(file_path)
        csv = csv or (ext == '.csv')

        if sheet_name:
            df = pd.read_excel(file_path, sheet_name, keep_default_na=False)
        else:
            df = pd.read_csv(file_path, **read_csv_args, keep_default_na=False) if csv \
                else pd.read_excel(file_path, keep_default_na=False)

        if one_column:
            split_values = [split for row in df.iloc[:, 0] if
                            (split := row.split(':', maxsplit=1)) and len(split) > 1]
            return dict(split_values)

        return dict(df.values)

    _DATASET_PATH = '/mnt/ssd/RAIS'

    cnae_subclass_codes_cana = {
        '0113000': 'Cultivo de Cana-De-Açúcar',
        '1071600': 'Fabricação de Açúcar em Bruto',
        '1072401': 'Fabricação de Açúcar de Cana Refinado',
        '1931400': 'Fabricação de álcool',
    }
    cnae_subclass_codes_cana_np = np.array(list(cnae_subclass_codes_cana.keys()))

    cbo_codes_cana = {
        '622110': 'Trabalhador da cultura de cana-de-açúcar',
        '621005': 'Trabalhador agropecuário em geral',
        '622020': 'Trabalhador volante da agricultura',
    }
    cbo_codes_cana_np = np.array(list(cbo_codes_cana.keys()))

    cnae_subclass_codes = {}
    cbo_codes = {}
    grau_instrucao = {}
    raca_cor = {}

    _rais_vinculos_sheet = os.path.join(_DATASET_PATH, 'dados', 'brutos', 'RAIS_vinculos_layout.xls')
    _cbo_sheet = os.path.join(_DATASET_PATH, 'dados', 'brutos', 'estrutura_CBO', 'CBO2002 - Ocupacao.csv')

    cnae_subclass_codes = generate_dict_from_spreadsheet(
        _rais_vinculos_sheet, sheet_name='subclasse 2.0', one_column=True
    )
    city_codes = generate_dict_from_spreadsheet(
        _rais_vinculos_sheet, sheet_name='municipio', one_column=True
    )
    cbo_codes = generate_dict_from_spreadsheet(
        _cbo_sheet, sep=';', encoding='latin-1', dtype='str'
    )

    grau_instrucao = {
        1: 'ANAFALBETO',
        2: 'ATE 5.A INC',
        3: '5.A CO FUND',
        4: '6. A 9. FUND',
        5: 'FUND COMPL',
        6: 'MEDIO INCOMP',
        7: 'MEDIO COMPL',
        8: 'SUP. INCOMP',
        9: 'SUP. COMP',
        10: 'MESTRADO',
        11: 'DOUTORADO'
    }

    raca_cor = {
        1: 'INDIGENA',
        2: 'BRANCA',
        4: 'PRETA',
        6: 'AMARELA',
        8: 'PARDA',
        9: 'NAO IDENTIFICADO' 
    }
    
    causa_afastamento = {
        10: 'ACI TRB TIP',
        20: 'ACI TRB TJT',
        30: 'DOEN REL TR',
        40: 'DOEN NREL TR',
        50: 'LIC MATERNID',
        60: 'SERV MILITAR',
        70: 'LIC SEM VENC',
        -1: 'IGNORADO'
    }

    tipo_vinculo = {
        10: 'CLT U/PJ IND',
        15: 'CLT U/PF IND',
        20: 'CLT R/PJ IND',
        25: 'CLT R/PF IND',
        30: 'ESTATUTARIO',
        31: 'ESTAT RGPS',
        35: 'ESTAT N/EFET',
        40: 'AVULSO',
        50: 'TEMPORARIO',
        55: 'APREND CONTR',
        60: 'CLT U/PJ DET',
        65: 'CLT U/PF DET',
        70: 'CLT R/PJ DET',
        75: 'CLT R/PF DET',
        80: 'DIRETOR',
        90: 'CONT PRZ DET',
        95: 'CONT TMP DET',
        96: 'CONT LEI EST',
        97: 'CONT LEI MUN'
    }

    tipo_salario = {
        1: 'MENSAL',
        2: 'QUINZENAL',
        3: 'SEMANAL',
        4: 'DIARIO',
        5: 'HORARIO',
        6: 'TAREFA',
        7: 'OUTROS'
    }

    tipo_deficiencia = {
        1: 'FISICA',
        2: 'AUDITIVA',
        3: 'VISUAL',
        4: 'MENTAL',
        5: 'MULTIPLA',
        6: 'REABILITADO',
        0: 'NAO DEFIC',
        -1: 'IGNORADO'
    }

    nacionalidade = {
        10: 'BRASILEIRA',
        20: 'NATUR BRAS',
        21: 'ARGENTINA',
        22: 'BOLIVIANA',
        23: 'CHILENA',
        24: 'PARAGUAIA',
        25: 'URUGUAIA',
        30: 'ALEMA',
        31: 'BELGA',
        32: 'BRITANICA',
        34: 'CANADENSE',
        35: 'ESPANHOLA',
        36: 'NORTE AMERIC',
        37: 'FRANCESA',
        38: 'SUICA',
        39: 'ITALIANA',
        41: 'JAPONESA',
        42: 'CHINESA',
        43: 'COREANA',
        45: 'PORTUGUESA',
        48: 'OUT LAT AMER',
        49: 'OUTR ASIATIC',
        50: 'OUTRAS NAC',
    }

    @staticmethod
    def get_cnae(code: str):
        return CnaeAndCboManager.cnae_subclass_codes.get(code)
    
    @staticmethod
    def get_cbo(code: str):
        return CnaeAndCboManager.cbo_codes.get(code)
    
    @staticmethod
    def get_grau_instrucao(code: int):
        return CnaeAndCboManager.grau_instrucao.get(code)
    
    @staticmethod
    def get_raca_cor(code: int):
        return CnaeAndCboManager.raca_cor.get(code)
    
    @staticmethod
    def get_city(code: int):
        return CnaeAndCboManager.city_codes.get(code)

    @staticmethod
    def is_cana_manual(cnae, cbo):
        return cnae in CnaeAndCboManager.cnae_subclass_codes_cana and \
               cbo in CnaeAndCboManager.cbo_codes_cana
    
    @staticmethod
    def is_cana_manual_np(cnaes, cbos):
        return np.isin(cnaes, CnaeAndCboManager.cnae_subclass_codes_cana_np) & \
            np.isin(cbos, CnaeAndCboManager.cbo_codes_cana_np)