import io
import pandas as pd
import requests
import gzip
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    base_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/'

    file_names = ['green_tripdata_2020-10.csv.gz', 'green_tripdata_2020-11.csv.gz', 'green_tripdata_2020-12.csv.gz']
    
    # prepare loop
    dfs = []
    # iterate over the files
    for file_name in file_names:
        
        file_url = base_url + file_name
        response = requests.get(file_url)
        
        # Check if the request was successful
        if response.status_code == 200:
        
            try:
                # deal with the compression of each file
                gz_content = io.BytesIO(response.content)
                
                with gzip.open(gz_content, 'rt') as f:
                    df = pd.read_csv(f, on_bad_lines='skip')
                    dfs.append(df)
            except Exception as e:
                print(f'Failed to process {file_name}: {e}')
        else:
            print(f'Failed to fetch {file_name}')
    concat_df = pd.concat(dfs, ignore_index=True)
        
    return concat_df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
