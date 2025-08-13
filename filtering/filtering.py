import os
import shutil
import logging
import signal
from pathlib import Path

from helpers import *
from dataset_reader import DatasetReader


def main():
    # signal handler for Ctrl+C
    signal.signal(signal.SIGINT, error_handler)

    data_dir = Path('/mnt/ssd/RAIS/dados/brutos')
    filtered_data_dir = Path('/mnt/ssd/RAIS/dados/filtrados')

    for root, _, files in os.walk(data_dir):
        if dir_should_be_ignored(root):
            continue

        for compressed_file in get_compressed_files(files):
            if file_should_be_ignored(compressed_file):
                continue

            handle_compressed_file(root, compressed_file, filtered_data_dir)


def dir_should_be_ignored(directory):
    ignore = ['legado', '2018', '2019', '2020']
    return any(i in directory.lower() for i in ignore)


def handle_compressed_file(root, compressed_file, filtered_data_dir):
    global current_chunks_dir, current_extracted_file, reader
    
    if 'RJ2008ID.7z' == compressed_file: # idk why this errors out. file is not used anyways.
        logging.info(f'ignoring {compressed_file}')
        return

    compressed_file_path = Path(root) / compressed_file
    inner_files = get_inner_files(str(compressed_file_path))

    # Inner file: e.g. RJ2008ID.TXT
    for inner_file in inner_files:
        if unwanted_file(inner_file):
            continue

        year = extract_number_from_filename(inner_file)
        state = extract_state_from_filename(inner_file)
        
        chunks_dir = Path(filtered_data_dir) / year / state
        chunks_dir.mkdir(parents=True, exist_ok=True)

        if has_been_processed(chunks_dir):
            logging.info(f'Skipping {inner_file} - state directory already processed')
            continue
        
        separator()
        logging.info(f'Extraindo: {inner_file}...')

        try:
            # Extract the file to the original location
            inner_file_path = extract_inner_file(root, str(compressed_file_path), inner_file)
            current_extracted_file = inner_file_path

            current_chunks_dir = chunks_dir

            reader.read_and_save_chunks(
                inner_file_path,
                str(chunks_dir),
                chunk_size=500000,
                year=year
            )

            os.remove(inner_file_path)
            current_extracted_file = None
            current_chunks_dir = None

        except Exception as e:
            logging.exception(f'Erro ao processar {inner_file}: {e}')
            error_handler(should_exit=False)
            current_extracted_file = None
            current_chunks_dir = None
            continue


def has_been_processed(state_dir: Path) -> bool:
    """Check if a state directory has been processed by looking for chunk files"""
    if not state_dir.exists():
        return False
    
    # Check if there are any chunk files in the directory
    chunk_files = list(state_dir.glob('chunk_*.parquet.zstd'))
    return len(chunk_files) > 0


def error_handler(signum=None, frame=None, should_exit=True):
    if current_chunks_dir and current_chunks_dir.exists():
        logging.info(f'\nCleaning up chunks directory: {current_chunks_dir}')
        shutil.rmtree(current_chunks_dir)
    if current_extracted_file and os.path.exists(current_extracted_file):
        logging.info(f'Cleaning up extracted file: {current_extracted_file}')
        os.remove(current_extracted_file)
    
    if should_exit:
        exit(0)


def setup_logging(log_file):
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.ERROR)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter('%(message)s'))

    logging.getLogger().setLevel(logging.INFO)
    logging.getLogger().addHandler(file_handler)
    logging.getLogger().addHandler(console_handler)


def separator():
    print('-' * 60)


current_chunks_dir = None
current_extracted_file = None
reader = DatasetReader()


if __name__ == '__main__':
    setup_logging('erros.txt')
    main()
