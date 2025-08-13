import os
import re
from typing import Callable, Optional

import numpy as np
import pandas as pd

from .column_mapping import ColumnMappingList


class DatasetReader:
    MAPPING_DATA = [
        # Possible names, New Name, Type
        ({'CPF'}, 'Cpf', str),
        ({'NOME', 'Nome Trabalhador'}, 'Nome', str),
        ({'Vínculo Ativo 31/12', 'EMP EM 31/12'}, 'VinculoAtivo31/12', bool),
        ({'CNAE 2.0 Subclasse', 'SB CLAS 20'}, 'CnaeSubclasse20', pd.CategoricalDtype()),
        ({'CBO Ocupação 2002', 'OCUP 2002'}, 'CboOcupacao2002', pd.CategoricalDtype()),
        ({'GR INSTRUCAO', 'Escolaridade após 2005'}, 'GrauInstrucao', np.int8),
        ({'GENERO', 'SEXO TRABALHADOR', 'Sexo Trabalhador'}, 'IsHomem', np.int8),
        ({'RACA_COR', 'Raça Cor'}, 'RacaCor', np.int8),
        ({'DT ADMISSAO', 'Data Admissão Declarada'}, 'DataAdmissao', str),
        ({'HORAS CONTR', 'Qtd Hora Contr'}, 'HorasContrato', np.int16),
        ({'Idade', 'DT NASCIMENT', 'Data de Nascimento'}, 'Idade', str),
        ({'MUNICIPIO', 'Município'}, 'Municipio', pd.CategoricalDtype()),
        ({'IDENTIFICAD', 'RADIC CNPJ', 'CNPJ / CEI'}, 'CNPJ', str),
        ({'TP Defic', 'Tipo Defic', 'TP DEFIC'}, 'Tipodeficiencia', np.int8),
        ({'NACIONALIDAD', 'Nacionalidade'}, 'Nacionalidade', np.int8),
        ({'CAUS AFAST 1', 'Causa Afastamento 1'}, 'Afastamentocausa1', np.int8),
        ({'CAUS AFAST 2', 'Causa Afastamento 2'}, 'Afastamentocausa2', np.int8),
        ({'CAUS AFAST 3', 'Causa Afastamento 3'}, 'Afastamentocausa3', np.int8),
        ({'QT DIAS AFAS', 'Qtd Dias Afastamento'}, 'Diasafastado', np.int8),
        ({'TIPO SAL', 'Tipo Salário'}, 'Tiposalario', np.int8),
        ({'TP VINCULO', 'Tipo Vínculo'}, 'Tipovinculo', np.int8),

        # dtype ignored for these. see _populate_transformations
        ({'REM DEZ (R$)', 'Vl Remun Dezembro Nom'}, 'RemuneracaoDezembroR$', np.float128),
        ({'REM DEZEMBRO', 'REM DEZ', 'Vl Remun Dezembro (SM)'}, 'RemuneracaoDezembro', np.float64),
        ({'REM MED (R$)', 'Vl Remun Média Nom'}, 'RemuneracaoMediaR$', np.float128),
        ({'REM MEDIA', 'REM MED', 'Vl Remun Média (SM)'}, 'RemuneracaoMedia', np.float64),
        ({'SAL CONTR', 'Vl Salário Contratual'}, 'SalarioContratual', np.float128),
    ]

    def __init__(self):
        self.column_mappings = ColumnMappingList.from_tuples(self.MAPPING_DATA) 
    
    def _read_csv(self, file_path: str, chunk_size: Optional[int] = None, skiprows: Optional[list[int]] = None):
        column_mappings = self.column_mappings
        
        csv_cols = self._get_csv_columns(file_path)
        column_mappings.update_current_names(csv_cols)
        
        columns_rename_map = column_mappings.get_column_rename_map()
        current_columns = set(columns_rename_map.keys())
        
        transformation_map = self._populate_transformations(file_path, current_columns)
        dtype_map = column_mappings.get_dtype_map()
        dtype_map = {k: v for k, v in dtype_map.items() if k not in transformation_map}
        
        has_age = 'Idade' in current_columns
        
        pd_return = pd.read_csv(file_path, sep=';', encoding='latin-1', usecols=current_columns,
                            converters=transformation_map, dtype=dtype_map, chunksize=chunk_size, skiprows=skiprows)

        return pd_return, columns_rename_map, has_age

    def read(self, file_path: str, year: Optional[int] = None, skiprows: Optional[list[int]] = None):
        if year is None:
            year = self._extract_year_from_filename(file_path)
        
        df, columns_rename_map, has_age = self._read_csv(file_path, skiprows=skiprows)
        df = self._post_process_dataframe(df, columns_rename_map, year, has_age)
        
        return df

    def read_and_save_chunks(self, file_path: str, output_dir: str, chunk_size: int = 10000,
                             year: Optional[int] = None, skiprows: Optional[list[int]] = None):
        os.makedirs(output_dir, exist_ok=True)
        
        if year is None:
            year = self._extract_year_from_filename(file_path)
        
        chunk_iterator, columns_rename_map, has_age = self._read_csv(file_path, chunk_size, skiprows=skiprows)
        
        for i, chunk in enumerate(chunk_iterator):
            chunk = self._post_process_dataframe(chunk, columns_rename_map, year, has_age)
            chunk_output_path = os.path.join(output_dir, f'chunk_{i}.parquet.zstd')
            chunk.to_parquet(chunk_output_path, index=False, compression='zstd')
    
    def _post_process_dataframe(self, df: pd.DataFrame, rename_map: dict[str, str], year: int, has_age: bool) -> pd.DataFrame:
        df = df.rename(columns=rename_map)

        age_relative_to = pd.Timestamp(year=int(year), month=12, day=31, hour=23, minute=59, second=59)
        if not has_age:
            df['Idade'] = self._calculate_age(df['Idade'].str.zfill(8), age_relative_to)
        
        for mapping in self.column_mappings:
            if isinstance(mapping.dtype, pd.CategoricalDtype):
                df[mapping.new_name] = df[mapping.new_name].astype(mapping.dtype)

        df['Idade'] = df['Idade'].astype(np.int8)
        df['IsHomem'] = df['IsHomem'].astype(np.int8)
        df['DataAdmissao'] = pd.to_datetime(df['DataAdmissao'].str.zfill(8), errors='coerce', format='%d%m%Y')

        return df

    def _calculate_age(self, birthdate: pd.Series, relative_to: pd.Timestamp):
        date_format = '%d%m%Y'

        birth = pd.to_datetime(birthdate, errors='coerce', format=date_format)
        birth = birth.fillna(
            pd.to_datetime('01' + birthdate.str[2:], format=date_format, errors='coerce') + pd.offsets.MonthEnd(0)
        )

        has_had_birthday = (relative_to.month > birth.dt.month) | ((relative_to.month == birth.dt.month) & (relative_to.day >= birth.dt.day))
        age = relative_to.year - birth.dt.year - (~has_had_birthday)

        return age.astype(np.int8)

    def _extract_year_from_filename(self, filename: str) -> int:
        """Extrai o ano do nome do arquivo. Exemplo: 'AL2014ID.csv' -> 2014"""

        filename = os.path.basename(filename)
        match = re.search(r'(\d+)(?=ID)', filename)

        if match is None:
            raise Exception(f'Could not extract year from {filename}')

        return int(match.group(1))

    def _get_csv_columns(self, file_path: str):
        """Obtém todas as colunas do CSV sem carregar os dados"""
        return set(pd.read_csv(file_path, nrows=0, sep=';', encoding='latin-1').columns)
    
    def _get_is_homem_transformation(self, file_path: str, current_columns: set[str]):
        """Retorna uma função que transforma a coluna IsHomem"""

        column = set(current_columns).intersection({'GENERO', 'SEXO TRABALHADOR', 'Sexo Trabalhador'})
        sample_value = pd.read_csv(file_path, sep=';', encoding='latin-1', usecols=column, nrows=1).iloc[0, 0]
        if str(sample_value).strip().isdigit():
            return lambda x: 0 if int(x) == 2 else int(x) # they use 1 for male, 2 for female, -1 for unidentified

        def transform(val):
            val = val.strip()[0].upper()
            if val == 'M':
                return 1
            if val == 'F':
                return 0
            return -1

        return transform
    
    def _populate_transformations(self, file_path: str, current_columns: set[str]) -> dict[str, Callable]:
        """Define transformações para algumas colunas"""

        def parse_money(x):
            x = x.replace(',', '.', 1)
            try:
                x = np.float32(x)
            except (ValueError, OverflowError):
                x = -1

            if x == np.inf:
                x = -1
            
            return x

        transformation_map = {
            'Cpf': lambda x: re.sub(r'\D', '', str(x)).zfill(11),
            'IsHomem': self._get_is_homem_transformation(file_path, current_columns),
            'SalarioContratual': lambda x: parse_money(x),
            'RemuneracaoMediaR$': lambda x: parse_money(x),
            'RemuneracaoMedia': lambda x: parse_money(x),
            'RemuneracaoDezembroR$': lambda x: parse_money(x),
            'RemuneracaoDezembro': lambda x: parse_money(x),
            'CboOcupacao2002': lambda x: re.sub(r'\D', '', str(x)),
            'CnaeSubclasse20': lambda x: re.sub(r'\D', '', str(x)),
        }

        self.column_mappings.populate_transformations(transformation_map)
        return self.column_mappings.get_transformation_map()
