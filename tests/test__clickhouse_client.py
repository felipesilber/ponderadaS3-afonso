import pytest
from data_pipeline.clickhouse_client import get_client, insert_dataframe
import pandas as pd
from unittest.mock import patch, MagicMock
from datetime import datetime

@patch('data_pipeline.clickhouse_client.clickhouse_connect.get_client')
def test_get_client(mock_get_client):
    mock_client = MagicMock()
    mock_get_client.return_value = mock_client

    client = get_client()
    assert client is not None
    mock_get_client.assert_called_once()

@patch('data_pipeline.clickhouse_client.clickhouse_connect.get_client')
def test_insert_dataframe(mock_get_client):
    mock_client = MagicMock()
    mock_get_client.return_value = mock_client

    df = pd.DataFrame({
        'data_ingestao': [datetime.now()],
        'dado_linha': [
            '{"id_employee":643,"name":"Antonella","surname":"da Cruz","cpf":"938.507.461-00"}'
        ],
        'tag': ['example_tag']
    })

    insert_dataframe(mock_client, 'test_table', df)

    mock_client.insert_df.assert_called_once_with('test_table', df)

    expected_columns = ['data_ingestao', 'dado_linha', 'tag']
    assert all(df.columns == expected_columns), "O DataFrame não contém as colunas esperadas."

    expected_json = '{"id_employee":643,"name":"Antonella","surname":"da Cruz","cpf":"938.507.461-00"}'
    assert df['dado_linha'].iloc[0] == expected_json, "O conteúdo de 'dado_linha' não está no formato esperado."
