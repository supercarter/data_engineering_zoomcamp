
from sqlalchemy import create_engine
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


@task
def read_data() -> pd.DataFrame:
    """"Read the CSV to a dataframe. Change this later to download csv from github"""
    color = 'green'
    year = 2019
    month = '01'
    df = pd.read_csv(f'week1_homework/{color}_tripdata_{year}-{month}.csv.gz')
    return df

@task
def clean_data(data: pd.DataFrame) -> pd.DataFrame:
    """Simple cleaning. Remove any records where passenger_count is 0"""
    data = data[data['passenger_count'] != 0]
    return data

@task
def upload_to_gcs(data: pd.DataFrame, port: str, dbname: str) -> None:
    """Move the cleaned data to google cloud storage bucket."""
    # Create a storage client
    storage_client = storage.Client()
    # Get bucket from client
    bucket = storage_client.bucket("green_taxi_csvs")
    # Upload
    blob = bucket.blob("green_taxi")
    blob.upload_from_file()
    connection_string = f"postgresql://root:root@localhost:{port}/{dbname}"
    engine = create_engine(connection_string)
    data.head(n=0).to_sql(name='green_taxi_data_gh', con=engine, if_exists='replace')
    data.to_sql(name='green_taxi_data_gh',con=engine,if_exists='append')


@flow
def ingest_data_flow():
    print("Starting flow...")
    df = read_data()
    df = clean_data(df)
    ingest_to_pg(df, '5432', 'ny_taxi')

if __name__ == "__main__":
    ingest_data_flow()



