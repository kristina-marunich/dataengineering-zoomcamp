import pandas as pd
import re

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def camel_to_snake(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


@transformer
def transform(data, *args, **kwargs):
    data = data[(data['passenger_count'] != 0)&(data['trip_distance'] != 0)]
    data['lpep_pickup_date'] = pd.to_datetime(data['lpep_pickup_datetime'])
    data.rename(columns=lambda x: camel_to_snake(x), inplace=True)
    data

    return data

@test
def test_vendor(data, *args) -> None:
    assert data['vendor_id'].isin([1, 2]).all(), "vendor_id should be equal to 1 or 2"



@test
def test_passengers(data, *args) -> None:
    assert data['passenger_count'].isin([0]).sum() == 0, 'There are no rides with zero passengers'

@test
def test_distance(data, *args) -> None:
    assert data['trip_distance'].isin([0]).sum() == 0, 'There are no rides with zero distance'





