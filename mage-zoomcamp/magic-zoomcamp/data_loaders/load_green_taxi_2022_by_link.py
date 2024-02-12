import io
import pandas as pd
import requests

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

#function to call individual parquet data and append them into one
def monthly_data():
    for page in range(1,13):
        base_url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-{page:02d}.parquet'
        print(f"Loading {page} months: {base_url}")
        yield pd.read_parquet(base_url)

@data_loader
def load_data_from_api(*args, **kwargs):
    df = pd.concat(monthly_data())
    return df