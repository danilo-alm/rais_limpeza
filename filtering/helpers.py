import os
import re
import sys

import py7zr

sys.path.append('../')
from processing.dataset_reader import DatasetReader


def change_root_directory(path, new_root):
    old_root = path.split(os.sep)[0] if path[0] != os.sep else path.split(os.sep)[1]
    return path.replace(old_root, new_root, 1)

def extract_number_from_filename(filename):
    # AL2014ID.txt -> 2014
    match = re.search(r'(\d+).*?ID', filename)
    if not match:
        raise ValueError('Número não encontrado no nome do arquivo')
    return match.group(1)

def extract_state_from_filename(filename):
    # AL2014ID.txt -> AL
    match = re.search(r'^([A-Z]{2})\d+ID', filename)
    if not match:
        raise ValueError('Estado não encontrado no nome do arquivo')
    return match.group(1)

def get_compressed_files(files):
    return [i for i in files if i.endswith('.7z')]

def file_should_be_ignored(filename):
    return 'estb' in filename.lower()

def get_inner_files(compressed_file):
    with py7zr.SevenZipFile(compressed_file, mode='r') as z:
        return z.getnames() or []

def get_new_year_path_from_filename(filename, filtered_data_dir):
    path = os.path.join(filtered_data_dir, extract_number_from_filename(filename))
    os.makedirs(path, exist_ok=True)
    return path

def get_extracted_file_new_location(filename, filtered_data_dir):
    year_path = get_new_year_path_from_filename(filename, filtered_data_dir)
    state = extract_state_from_filename(filename)
    filename = os.path.splitext(filename)[0] + '.csv'
    return os.path.join(year_path, state, filename)

def get_target_directory(filename, filtered_data_dir):
    year_path = get_new_year_path_from_filename(filename, filtered_data_dir)
    os.makedirs()

def get_equivalent_parquet_file(file_path):
    # This function is no longer used directly, but keeping it for backward compatibility
    # It now returns the same path as the input
    return file_path

def unwanted_file(file):
    return file.startswith('sp08')

def extract_inner_file(root, compressed_file_path, inner_file):
    with py7zr.SevenZipFile(compressed_file_path, mode='r') as z:
        z.extract(path=root, targets=[inner_file])
    return os.path.join(root, inner_file)

def read_and_process_csv(path, inner_file, logger):
    try:
        df = DatasetReader.read_csv(
            path,
            year=extract_number_from_filename(inner_file)
        )
    except Exception as e:
        logger.error(f'Erro ao ler {path}: {e}')
        return
    finally:
        os.remove(path)
    return df
