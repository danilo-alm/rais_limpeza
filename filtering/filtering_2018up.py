import os
import logging
from pathlib import Path

import pandas as pd
import numpy as np

from helpers import (
    get_compressed_files, get_inner_files, extract_inner_file
)
from dataset_reader import DatasetReader
from cnae_and_cbo_manager import CnaeAndCboManager


NUM_BRAZIL_STATES = 27


def dir_should_be_ignored(directory):
    ignore = ['legado'] + [str(i) for i in range(2008, 2018)]
    return any(i in directory.lower() for i in ignore)


def main():
    data_dir = Path('/mnt/ssd/RAIS/dados/brutos')
    filtered_data_dir = Path('/mnt/ssd/RAIS/dados/filtrados')

    city_to_state = {
        key: val.split('-')[0].upper()
        for key, val in CnaeAndCboManager.city_codes.items()
    }

    reader = DatasetReader()

    for root, _, files in os.walk(data_dir):
        if dir_should_be_ignored(root):
            continue

        root = Path(root)

        for compressed_file in get_compressed_files(files):
            year = extract_year_from_path(root)
            year_dir = filtered_data_dir / year
            year_dir.mkdir(exist_ok=True)

            if len(list(year_dir.iterdir())) == NUM_BRAZIL_STATES:
                print(f'{year} already filtered')
                break

            compressed_file_path = root / compressed_file
            inner_file = get_inner_files(compressed_file_path)[0]
            inner_file_path = root / inner_file

            if not inner_file_path.exists():
                print(f'extracting {compressed_file_path}...')
                inner_file_path = extract_inner_file(root, compressed_file_path, inner_file)

            print(f'reading states from {inner_file_path}...')
            municipio_series = pd.read_csv(
                inner_file_path,
                sep=';',
                encoding='latin1',
                usecols=['Município'],
                dtype=str
            ).squeeze()

            estados = municipio_series.map(city_to_state).rename('Estado')

            for estado in estados.unique():
                estado_output_dir = year_dir / estado

                if estado_output_dir.exists():
                    print(f'{estado} ({year}) already filtered')
                    continue

                skip = np.concatenate([[False], estados != estado])  # keep header

                print(f'\nprocessing {estado} ({year}) from {inner_file_path}...')
                try:
                    reader.read_and_save_chunks(
                        file_path=inner_file_path,
                        output_dir=str(estado_output_dir),
                        chunk_size=500000,
                        year=year,
                        skiprows=lambda x: skip[x]
                    )
                except Exception as e:
                    logging.exception(f'Erro ao processar {estado} ({year}): {e}')
                    continue

            print(f'removing {inner_file_path}...')
            os.remove(inner_file_path)
            print('-' * 40)


def extract_year_from_path(path):
    for part in Path(path).parts:
        if part.isdigit() and len(part) == 4:
            return part
    raise ValueError(f"Could not extract year from path: {path}")


if __name__ == '__main__':
    main()
