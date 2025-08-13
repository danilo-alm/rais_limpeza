import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.compute as pc


def load_dataset(year: int = None, state: str = None, cpfs: list[str] = None,
                 head=None, path=None) -> pd.DataFrame:

    if path is None:
        if state is None or year is None:
            raise ValueError('State must be provided if path is not specified.')
        path = FILTERED_DATA_DIR / f'{year}' / f'{state}'

    dataset = ds.dataset(path, format='parquet')

    if head:
        return dataset.head(head).to_pandas()

    myfilter = None

    if cpfs:
        array = pa.array(cpfs)
        myfilter = pc.is_in(pc.field('Cpf'), array)
        dataset = dataset.filter(myfilter)
    
    return dataset.to_table(filter=myfilter).to_pandas()


FILTERED_DATA_DIR = 'caminho/para/os/dados/filtrados'
