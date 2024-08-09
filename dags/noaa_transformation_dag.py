import calendar
import json
import logging
from datetime import datetime, timedelta
from io import BytesIO

import pandas as pd
import requests
from botocore.exceptions import ClientError
from sqlalchemy import MetaData, Table, select
from sqlalchemy.dialects.postgresql import insert

from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize the DAG with catchup=False to prevent backfilling
dag = DAG(
    'climate_data_pipeline',
    default_args=default_args,
    description='A simple climate data pipeline',
    schedule_interval=timedelta(days=1),  # Run once a day
    catchup=False,  # Prevent backfilling
)

# Define global variables
STATIONS = ["GHCND:USW00094728", "GHCND:USW00014739"]  # Add more station IDs as needed
YEAR = 2023
S3_BUCKET = 'noaa-data-atc-2024'
RAW_S3_PREFIX = 'raw-data'
CLEANED_S3_PREFIX = 'clean-data'

# Function to fetch data from the API
def fetch_data_from_api(station_id, start_date, end_date):
    url = f'https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid=GHCND&stationid={station_id}&startdate={start_date}&enddate={end_date}&limit=1000'
    headers = {'token': Variable.get("NOAA_API_TOKEN")}
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        if 'results' in data:
            return data['results']
        else:
            print(f"No 'results' key found in the response for station {station_id}")
            return []
    else:
        print(f"Failed to fetch data for station {station_id}. Status code: {response.status_code}, Response: {response.text}")
        return []

# Task: Obtain data from the source and save raw data
def obtain_data(**kwargs):
    s3_hook = S3Hook('noaa-s3-bucket')

    for station in STATIONS:
        for month in range(1, 13):
            start_date = f"{YEAR}-{month:02d}-01"
            end_date = f"{YEAR}-{month:02d}-{calendar.monthrange(YEAR, month)[1]}"
            data = fetch_data_from_api(station, start_date, end_date)
            file_path = f'{RAW_S3_PREFIX}/{station}/data-{YEAR}-{month:02d}.json'
            
            if data:
                s3_hook.load_string(json.dumps(data), key=file_path, bucket_name=S3_BUCKET, replace=True)

obtain_data_task = PythonOperator(
    task_id='obtain_data',
    python_callable=obtain_data,
    dag=dag,
)

obtain_data_task

# Function to list all keys in the S3 bucket
def list_keys_in_s3(bucket_name, prefix):
    s3_hook = S3Hook('noaa-s3-bucket')
    keys = s3_hook.list_keys(bucket_name=bucket_name, prefix=prefix)
    return keys

# Function to list and read raw data from S3
def list_and_read_raw_data_from_s3(station_id, year, month):
    s3_hook = S3Hook('noaa-s3-bucket')
    key = f'{RAW_S3_PREFIX}/{station_id}/data-{year}-{month:02d}.json'
    try:
        data = s3_hook.read_key(key, bucket_name=S3_BUCKET)
        return json.loads(data)
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            logging.info(f"File not found: {key}")
            return None
        else:
            raise

# Function to clean and transform data
def clean_and_transform_data(raw_data):
    df = pd.DataFrame(raw_data)

    # Transform the data according to the table structures
    df['date'] = pd.to_datetime(df['date'])
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month
    df['day'] = df['date'].dt.day
    df['day_of_week'] = df['date'].dt.day_name()
    df['week_of_year'] = df['date'].dt.isocalendar().week
    df['quarter'] = df['date'].dt.quarter

    # Example of cleaning: replace NaNs with a default value or drop rows with NaNs
    df = df.dropna(subset=['date', 'station', 'datatype', 'value'])

    # Create separate DataFrames for each dimension table
    dim_date = df[['date', 'year', 'month', 'day', 'day_of_week', 'week_of_year', 'quarter']].drop_duplicates().reset_index(drop=True)
    dim_station = df[['station']].drop_duplicates().reset_index(drop=True).rename(columns={'station': 'station_id'})
    dim_datatype = df[['datatype']].drop_duplicates().reset_index(drop=True).rename(columns={'datatype': 'datatype_id'})

    fact_weather = df[['date', 'station', 'datatype', 'value', 'attributes']].copy()
    fact_weather = fact_weather.rename(columns={
        'station': 'station_id',
        'datatype': 'datatype_id'
    })

    return dim_date, dim_station, dim_datatype, fact_weather

# Function to save cleaned data to S3 as Parquet
def save_cleaned_data_to_s3(dim_date, dim_station, dim_datatype, fact_weather, year, month, station_id):
    s3_hook = S3Hook('noaa-s3-bucket')

    for df, table_name in zip([dim_date, dim_station, dim_datatype, fact_weather],
                              ['dim_date', 'dim_station', 'dim_datatype', 'fact_weather']):
        buffer = BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        key = f'{CLEANED_S3_PREFIX}/{table_name}/{station_id}-{year}-{month:02d}.parquet'
        s3_hook.load_file_obj(buffer, key=key, bucket_name=S3_BUCKET, replace=True)



# Task: Process raw data, clean, transform, and load to PostgreSQL
def process_transform_load_data(**kwargs):
    for station in STATIONS:
        for month in range(1, 13):
            raw_data = list_and_read_raw_data_from_s3(station, YEAR, month)
            if raw_data:
                dim_date, dim_station, dim_datatype, fact_weather = clean_and_transform_data(raw_data)
                save_cleaned_data_to_s3(dim_date, dim_station, dim_datatype, fact_weather, YEAR, month, station)
            else:
                print("No file")


# Create the task to process raw data
process_data_task = PythonOperator(
    task_id='process_transform_load_data',
    python_callable=process_transform_load_data,
    dag=dag,
)

process_data_task

# Function to load data from S3 Parquet files to PostgreSQL
def load_data_to_postgresql_from_s3(table_name, station_id, year, month):
    s3_hook = S3Hook('noaa-s3-bucket')
    pg_hook = PostgresHook(postgres_conn_id='noaa-db')

    key = f'{CLEANED_S3_PREFIX}/{table_name}/{station_id}-{year}-{month:02d}.parquet'
    try:
        obj = s3_hook.get_key(key, bucket_name=S3_BUCKET)
        data = obj.get()["Body"].read()
        df = pd.read_parquet(BytesIO(data))
        
        engine = pg_hook.get_sqlalchemy_engine()
        conn = engine.connect()
        metadata = MetaData()

        # Reflect the table structures from the database
        table = Table(table_name, metadata, autoload_with=engine)
        dim_date_table = Table('dim_date', metadata, autoload_with=engine)

        if table_name == 'fact_weather':
            for index, row in df.iterrows():
                # Look up the date_id in dim_date
                date_lookup = select([dim_date_table.c.date_id]).where(dim_date_table.c.date == row['date'])
                date_id = conn.execute(date_lookup).scalar()

                # Ensure date_id was found
                if date_id is None:
                    raise ValueError(f"No date_id found for date {row['date']}")

                # Prepare the row for insertion into fact_weather
                fact_row = {
                    'date_id': date_id,
                    'station_id': row['station_id'],
                    'datatype_id': row['datatype_id'],
                    'value': row['value'],
                    'attributes': row['attributes']
                }

                # Insert into fact_weather with ON CONFLICT DO NOTHING
                stmt = insert(table).values(**fact_row)
                stmt = stmt.on_conflict_do_nothing(index_elements=['date_id', 'station_id', 'datatype_id'])
                conn.execute(stmt)

        else:
            for index, row in df.iterrows():
                if table_name == 'dim_date':
                    stmt = insert(table).values(
                        date=row['date'],
                        year=row['year'],
                        month=row['month'],
                        day=row['day'],
                        day_of_week=row['day_of_week'],
                        week_of_year=row['week_of_year'],
                        quarter=row['quarter']
                    )
                    stmt = stmt.on_conflict_do_nothing(index_elements=['date'])

                elif table_name == 'dim_station':
                    stmt = insert(table).values(station_id=row['station_id'])
                    stmt = stmt.on_conflict_do_nothing(index_elements=['station_id'])

                elif table_name == 'dim_datatype':
                    stmt = insert(table).values(datatype_id=row['datatype_id'])
                    stmt = stmt.on_conflict_do_nothing(index_elements=['datatype_id'])

                conn.execute(stmt)

    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            logging.info(f"File not found: {key}")
        else:
            raise
    finally:
        conn.close()

# Task: Load data to PostgreSQL
def load_data_to_postgresql(**kwargs):
    for station in STATIONS:
        for month in range(1, 13):
            for table_name in ['dim_date', 'dim_station', 'dim_datatype', 'fact_weather']:
                load_data_to_postgresql_from_s3(table_name, station, YEAR, month)

load_data_task = PythonOperator(
    task_id='load_data_to_postgresql',
    python_callable=load_data_to_postgresql,
    dag=dag,
)

load_data_task


obtain_data_task >> process_data_task >> load_data_task
