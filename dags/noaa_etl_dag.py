from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

import requests
import json
from datetime import datetime, timedelta

import io
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

# Function to fetch data from the API
def fetch_data_from_api(datasetid, stationid, locationid, startdate, enddate, s3_bucket, **kwargs):
    # Format the stationid and locationid according to the API requirements
    formatted_stationid = f"{datasetid}:{stationid}"
    formatted_locationid = f"CITY:{locationid}"

    url = f"https://www.ncei.noaa.gov/cdo-web/api/v2/data"
    headers = {'token': Variable.get("NOAA_API_TOKEN")}
    params = {
        'datasetid': datasetid,
        'stationid': formatted_stationid,
        'locationid': formatted_locationid,
        'startdate': startdate,
        'enddate': enddate,
        'limit': 1000
    }

    # Construct the full URL with parameters for debugging
    request_url = requests.Request('GET', url, params=params).prepare().url
    print(f"Requesting URL: {request_url}")  # Print the URL for debugging

    response = requests.get(url, headers=headers, params=params)
    data = response.json()

    for record in data.get('results', []):
        record_date = record['date'][:10]
        year, month, day = record_date.split('-')

        s3_key = f"raw-data/{datasetid}/{stationid}/{locationid}/{year}/{month}/{day}/result-{record_date}.json"
        json_data = json.dumps(record)

        s3_hook = S3Hook(aws_conn_id='noaa-s3-bucket')
        s3_hook.load_string(json_data, key=s3_key, bucket_name=s3_bucket, replace=True)

# Function to fetch parameters from the PostgreSQL database
def get_parameters_from_db(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='noaa-db')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute("""
        SELECT
            ds.dataset_id,
            dl.city_id,
            st.station_id
        FROM
            dim_stations st
        JOIN
            dim_locations dl ON st.city_id = dl.city_id
        JOIN
            dim_datasets ds ON st.dataset_id = ds.dataset_id;
    """)
    results = cursor.fetchall()

    params_list = []
    for result in results:
        dataset_id, city_id, station_id = result
        params_list.append({
            'datasetid': dataset_id,
            'stationid': station_id,
            'locationid': city_id
        })

    kwargs['ti'].xcom_push(key='params_list', value=params_list)

# Function to fetch and store data for each parameter set
def fetch_and_store_data(**kwargs):
    params_list = kwargs['ti'].xcom_pull(key='params_list')
    for params in params_list:
        fetch_data_from_api(
            datasetid=params['datasetid'],
            stationid=params['stationid'],
            locationid=params['locationid'],
            startdate=kwargs['startdate'],
            enddate=kwargs['enddate'],
            s3_bucket=kwargs['s3_bucket']
        )


# Function to process a single JSON file and convert it to Parquet
def process_and_convert_to_parquet(s3_bucket, input_key, output_key, aws_conn_id='aws_default'):
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)

    # Read the JSON file from S3 using S3Hook
    s3_object = s3_hook.get_key(key=input_key, bucket_name=s3_bucket)
    json_data = pd.read_json(io.BytesIO(s3_object.get()['Body'].read()), lines=True)

    # Convert the 'date' field to datetime
    json_data['date'] = pd.to_datetime(json_data['date'])

    # Extract additional date-related fields
    json_data['year'] = json_data['date'].dt.year
    json_data['month'] = json_data['date'].dt.month
    json_data['day'] = json_data['date'].dt.day
    json_data['day_of_week'] = json_data['date'].dt.dayofweek
    json_data['week_of_year'] = json_data['date'].dt.isocalendar().week
    json_data['quarter'] = json_data['date'].dt.quarter

    # Extract station ID from 'station' field
    json_data['station'] = json_data['station'].apply(lambda x: x.split(':')[1] if ':' in x else x)

    # Reorder the columns as required
    json_data = json_data[['date', 'year', 'month', 'day', 'day_of_week', 'week_of_year', 'quarter', 'datatype', 'station', 'attributes', 'value']]

    # Convert DataFrame to Parquet
    table = pa.Table.from_pandas(json_data)
    parquet_buffer = io.BytesIO()
    pq.write_table(table, parquet_buffer)

    # Upload the Parquet file back to S3
    parquet_buffer.seek(0)
    s3_hook.load_file_obj(parquet_buffer, key=output_key, bucket_name=s3_bucket, replace=True)

# Function to iterate over all files in a specific folder structure
def convert_json_files_in_s3(s3_bucket, raw_data_prefix, clean_data_prefix, aws_conn_id='aws_default'):
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)

    # List all JSON files in the raw data folder
    objects = s3_hook.list_keys(bucket_name=s3_bucket, prefix=raw_data_prefix)
    for input_key in objects:
        if input_key.endswith('.json'):
            # Derive the output key for the clean data folder
            output_key = input_key.replace('raw-data', 'clean-data').replace('.json', '.parquet')
            
            # Process and convert the file
            process_and_convert_to_parquet(s3_bucket, input_key, output_key, aws_conn_id)


# Function to insert date data into dim_date and retrieve date_id
def insert_and_get_date_id(row, pg_hook):
    insert_date_query = """
    INSERT INTO dim_date (date, year, month, day, day_of_week, week_of_year, quarter)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (date) DO NOTHING
    RETURNING date_id;
    """
    
    select_date_id_query = "SELECT date_id FROM dim_date WHERE date = %s"
    
    date_data = (
        row['date'],
        row['year'],
        row['month'],
        row['day'],
        row['day_of_week'],
        row['week_of_year'],
        row['quarter']
    )
    
    # Try to insert the date, or select the date_id if it already exists
    result = pg_hook.get_first(insert_date_query, parameters=date_data)
    
    if result:
        return result[0]  # Return the newly inserted date_id
    else:
        # If the date already exists, fetch the date_id
        return pg_hook.get_first(select_date_id_query, parameters=(row['date'],))[0]


# Function to process and insert data into both dim_date and fact_weather tables
def process_and_insert_data(s3_bucket, input_key, aws_conn_id='aws_default', postgres_conn_id='postgres_default'):
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    # Read the Parquet file from S3
    s3_object = s3_hook.get_key(key=input_key, bucket_name=s3_bucket)
    parquet_data = pd.read_parquet(io.BytesIO(s3_object.get()['Body'].read()))

    # Prepare the data for insertion into dim_date and fact_weather
    rows_to_insert = []

    for index, row in parquet_data.iterrows():
        # Insert into dim_date and get the date_id
        date_id = insert_and_get_date_id(row, pg_hook)

        # Directly use the datatype and station from the Parquet file
        datatype_id = row['datatype']
        station_id = row['station']

        # Prepare the row for insertion into fact_weather
        rows_to_insert.append((
            date_id,
            datatype_id,
            station_id,
            row['attributes'],
            row['value']
        ))

    # Insert rows into the fact_weather table
    insert_query = """
        INSERT INTO fact_weather (date_id, datatype_id, station_id, attributes, value)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (date_id, station_id, datatype_id)
        DO NOTHING;
    """
    pg_hook.insert_rows(table='fact_weather', rows=rows_to_insert, target_fields=['date_id', 'datatype_id', 'station_id', 'attributes', 'value'])


# Function to iterate over all files in a specific folder structure
def insert_parquet_files_to_tables(s3_bucket, clean_data_prefix, aws_conn_id='aws_default', postgres_conn_id='postgres_default'):
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)

    # List all Parquet files in the clean data folder
    objects = s3_hook.list_keys(bucket_name=s3_bucket, prefix=clean_data_prefix)
    for input_key in objects:
        if input_key.endswith('.parquet'):
            # Process and insert the file into the tables
            process_and_insert_data(s3_bucket, input_key, aws_conn_id, postgres_conn_id)




# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'noaa_api_etl_dag',
    default_args=default_args,
    description='A DAG to load data from NOAA API to S3',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 8, 11),  # DAG start date
    catchup=False,
)

# Task to get parameters from the PostgreSQL database
get_params_task = PythonOperator(
    task_id='get_parameters_from_db',
    python_callable=get_parameters_from_db,
    provide_context=True,
    dag=dag,
)

# Task to fetch and store data
fetch_and_store_task = PythonOperator(
    task_id='fetch_and_store_data',
    python_callable=fetch_and_store_data,
    op_kwargs={
        'startdate': '2023-01-01',  # Static start date for data fetching
        'enddate': '2023-05-31',     # Static end date for data fetching
        's3_bucket': Variable.get("NOAA_S3_BUCKET")
    },
    provide_context=True,
    dag=dag,
)

# Define the PythonOperator to run the conversion function
convert_task = PythonOperator(
    task_id='convert_json_to_parquet',
    python_callable=convert_json_files_in_s3,
    op_kwargs={
        's3_bucket': Variable.get("NOAA_S3_BUCKET"),
        'raw_data_prefix': 'raw-data/GHCND/',
        'clean_data_prefix': 'clean-data/GHCND/',
        'aws_conn_id': 'noaa-s3-bucket'
    },
    dag=dag,
)

# Define the PythonOperator to run the insertion function
insert_task = PythonOperator(
    task_id='insert_parquet_to_fact_and_dim',
    python_callable=insert_parquet_files_to_tables,
    op_kwargs={
        's3_bucket':  Variable.get("NOAA_S3_BUCKET"),
        'clean_data_prefix': 'clean-data/GHCND/',
        'aws_conn_id': 'noaa-s3-bucket',
        'postgres_conn_id': 'noaa-db'
    },
    dag=dag,
)

insert_task

get_params_task >> fetch_and_store_task >> convert_task >> insert_task
