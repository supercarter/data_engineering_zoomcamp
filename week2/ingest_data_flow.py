
from sqlalchemy import create_engine
import pandas as pd
from prefect import flow, task
import json
import requests
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
def write_to_local(data: pd.DataFrame, color: str) -> None:
    """Load the dataframe to local parquet"""
    data.to_parquet(f"week2/taxi-data/{color}_taxi.parquet")

@task
def upload_to_gcs(data: pd.DataFrame, color: str) -> None:
    """Move the cleaned data to google cloud storage bucket."""
    # Create a storage client
    gcp_bucket= GcsBucket.load("gcs-csv-storage")
    gcp_bucket.upload_from_path(from_path=f"week2/taxi-data/{color}_taxi.parquet",to_path="taxi_csvs/")

@task
def airbyte_gcs_to_bq() -> None:
    """Use airbyte api to move from gcs to bq"""
    with open("secrets/airbyte_secrets.json", "r") as f:
        config = json.load(f)
    connection_id = config["connection_id"]
    api_key = config["api_key"]
    url = config["URL"]
    
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type":  "application/json"
    }

    payload = {
        "connectionId": f"{connection_id}"
    }
    
    requests.post(url,headers=headers,data=payload)
    


@flow
def ingest_data_flow():
    print("Starting flow...")
    df = read_data()
    df = clean_data(df)
    write_to_local(df, 'green')
    upload_to_gcs(df, 'green')

if __name__ == "__main__":
    ingest_data_flow()



