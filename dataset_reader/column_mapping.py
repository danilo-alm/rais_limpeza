from typing import List, Tuple, Callable, Optional, Union

import numpy as np

PandasDType = Union[np.dtype, str]

class ColumnMapping:
    def __init__(self, possible_names: set[str], new_name: str, dtype: PandasDType):
        self.possible_names = possible_names
        self.new_name = new_name
        self.dtype = dtype

        self.transformation: Optional[Callable] = None
        self.current_name: str = ''
    
    @staticmethod
    def from_tuple(data: Tuple[set[str], str, PandasDType]) -> 'ColumnMapping':
        return ColumnMapping(*data)
    
class ColumnMappingList:
    def __init__(self, column_mappings: List[ColumnMapping]):
        self._column_mappings = column_mappings
        self._mapping_dict = {mapping.new_name: mapping for mapping in column_mappings}
    
    @staticmethod
    def from_tuples(column_mappings: List[Tuple[set[str], str, PandasDType]]) -> 'ColumnMappingList':
        return ColumnMappingList([ColumnMapping.from_tuple(mapping) for mapping in column_mappings])

    def get_by_current_name_in(self, names: List[str]) -> Optional[ColumnMapping]:
        '''Retorna o mapeamento que tem o current_name em names'''
        for mapping in self._column_mappings:
            if mapping.current_name in names:
                return mapping
        return None

    def update_current_names(self, columns: set[str]) -> Optional[ColumnMapping]:
        no_matches = []
        for mapping in self._column_mappings:
            match = mapping.possible_names.intersection(columns)
            match = next(iter(match), None)
            if match is None:
                no_matches.append(mapping)
                continue
            mapping.current_name = match

        if not no_matches:
            return

        error_message = 'No match for mapping(s) with new name and possible names:\n\nNew Name | Possible Names'
        for mapping in no_matches:
            error_message += f'\n{mapping.new_name} | {", ".join(mapping.possible_names)}'
        error_message += f'\n\nCSV Columns: {columns}'

        raise Exception(error_message)

    def get_transformation_map(self, new_names_as_keys: bool = False) -> dict[str, Callable]:
        '''Retorna um dicionário com os nomes atuais ou novos como chave e as transformações como valor.'''
        transformation_map = self._get_attribute_map('transformation', new_names_as_keys)
        return {k: v for k, v in transformation_map.items() if v is not None}

    def get_dtype_map(self, new_names_as_keys: bool = False) -> dict[str, str]:
        '''Retorna um dicionário com os nomes atuais ou novos como chave e os tipos de dados (dtype) como valor.'''
        return self._get_attribute_map('dtype', new_names_as_keys)
    
    def populate_transformations(self, transformation_dict: dict[str, Callable]) -> None:
        '''Popula as transformações para as colunas mapeadas.'''
        for column_name, transformation in transformation_dict.items():
            self[column_name].transformation = transformation

    def get_column_rename_map(self):
        return {mapping.current_name: mapping.new_name for mapping in self._column_mappings}

    def _get_attribute_map(self, attribute: str, new_names_as_keys: bool) -> dict:
        '''Retorna um dicionário onde a chave pode ser o nome atual ou novo, e o valor é o atributo especificado.'''
        key_selector = lambda mapping: mapping.new_name if new_names_as_keys else mapping.current_name
        return {key_selector(mapping): getattr(mapping, attribute) for mapping in self._column_mappings}

    def __getitem__(self, key: str) -> ColumnMapping:
        return self._mapping_dict[key]
    
    def __iter__(self):
        return iter(self._column_mappings)
