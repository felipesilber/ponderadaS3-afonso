from datetime import datetime
from data_pipeline.minio_client import create_bucket_if_not_exists, upload_file, download_file
from data_pipeline.clickhouse_client import execute_sql_script, get_client, insert_dataframe
from data_pipeline.data_processing import process_data, prepare_dataframe_for_insert
import pandas as pd

create_bucket_if_not_exists("raw-data")

execute_sql_script('sql/create_table.sql')

def process_csv():
    df = pd.read_csv('dados/employee_final.csv', encoding='latin1')

    filename = process_data(df)
    upload_file('raw-data', filename)

    download_file('raw-data', filename, f"downloaded_{filename}")
    df_parquet = pd.read_parquet(f"downloaded_{filename}")

    df_prepared = prepare_dataframe_for_insert(df_parquet)
    client = get_client()
    insert_dataframe(client, 'working_data', df_prepared)

execute_sql_script('sql/create_view.sql')

if __name__ == "__main__":
    process_csv()