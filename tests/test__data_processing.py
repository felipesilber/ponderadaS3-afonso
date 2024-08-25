import pytest
import pandas as pd
import os
from datetime import datetime
from unittest.mock import patch
from data_pipeline.data_processing import process_data, prepare_dataframe_for_insert

@patch('data_pipeline.data_processing.datetime')
def test_process_data(mock_datetime):
    mock_datetime.now.return_value = datetime(2023, 8, 23, 15, 45, 0)
    mock_datetime.strftime = datetime.strftime
    
    data = pd.DataFrame({ "id_employee": [643],
    "name": ["Antonella"],
    "surname": ["da Cruz"],
    "cpf": ["938.507.461-00"]})
    
    filename = process_data(data)
    
    assert filename == "raw_data_20230823154500.parquet"
    
    assert os.path.exists(filename)
    
    os.remove(filename)

@patch('data_pipeline.data_processing.datetime')
def test_prepare_dataframe_for_insert(mock_datetime):
    mock_datetime.now.return_value = datetime(2023, 8, 23, 15, 45, 0)
    
    data = pd.DataFrame({ "id_employee": [643],
    "name": ["Antonella"],
    "surname": ["da Cruz"],
    "cpf": ["938.507.461-00"]})
    
    result_df = prepare_dataframe_for_insert(data)
    
    assert 'data_ingestao' in result_df.columns
    assert 'dado_linha' in result_df.columns
    assert 'tag' in result_df.columns

    print('result_df',result_df.iloc[0])
    print('data', data.iloc[0].to_json)
    
    assert result_df.iloc[0]['tag'] == 'example_tag'
