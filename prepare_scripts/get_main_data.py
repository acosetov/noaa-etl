import requests
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get NOAA API token from environment variable
TOKEN = os.getenv('TOKEN')

# Base URL for the NOAA API
BASE_URL = 'https://www.ncei.noaa.gov/cdo-web/api/v2/'

# Headers for authentication
HEADERS = {'token': TOKEN}
DATA_SET_ID = 'GHCND'

# Function to fetch city locations with filtering
def fetch_cities(dataset_id, location_category_id, limit=1000):
    params = {
        'datasetid': dataset_id,
        'locationcategoryid': location_category_id,
        'limit': limit
    }
    url = f"{BASE_URL}locations"
    response = requests.get(url, headers=HEADERS, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching data: {response.status_code}, {response.text}")
        return None

# Function to filter cities based on the required criteria
def filter_cities(cities, city_names, max_date):
    filtered_cities = []
    for city in cities:
        if any(name in city['name'] for name in city_names):
            if city['maxdate'] >= max_date:
                filtered_cities.append(city)
    return filtered_cities

# Function to fetch stations for a given city
def fetch_stations(dataset_id, city_id, limit=1000):
    params = {
        'datasetid': dataset_id,
        'locationid': city_id,
        'limit': limit
    }
    url = f"{BASE_URL}stations"
    response = requests.get(url, headers=HEADERS, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching data: {response.status_code}, {response.text}")
        return None

# Function to fetch data types
def fetch_data_types(dataset_id, limit=1000):
    params = {
        'datasetid': dataset_id,
        'limit': limit
    }
    url = f"{BASE_URL}datatypes"
    response = requests.get(url, headers=HEADERS, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching data: {response.status_code}, {response.text}")
        return None

# Function to generate SQL insert statements for data types
def generate_sql_inserts_data_types(data_types):
    sql_statements = []
    for datatype in data_types:
        datatype_id = datatype['id']
        description = datatype['name'].replace("'", "''")  # Escape single quotes
        sql = f"INSERT INTO dim_datatypes (datatype_id, description) VALUES ('{datatype_id}', '{description}');"
        sql_statements.append(sql)
    return sql_statements


# Function to select the first station that matches the max_date criteria
def select_station(stations, max_date):
    for station in stations:
        if station['maxdate'].startswith(max_date):
            return station
    return None

# Function to generate SQL insert statements for cities
def generate_sql_inserts_cities(cities, dataset_id):
    sql_statements = []
    for city in cities:
        city_id = city['id'].split(":")[1]
        name = city['name'].replace("'", "''")  # Escape single quotes
        mindate = city['mindate']
        maxdate = city['maxdate']
        datacoverage = city['datacoverage']
        sql = f"INSERT INTO dim_locations (city_id, dataset_id, mindate, maxdate, name, datacoverage) " \
              f"VALUES ('{city_id}', '{dataset_id}', '{mindate}', '{maxdate}', '{name}', {datacoverage:.4f});"
        sql_statements.append(sql)
    return sql_statements

# Function to generate SQL insert statements for stations
def generate_sql_inserts_stations(station, city_id, dataset_id=DATA_SET_ID):
    station_id = station['id'].split(":")[1]
    name = station['name'].replace("'", "''")  # Escape single quotes
    latitude = station['latitude']
    longitude = station['longitude']
    elevation = station['elevation']
    elevation_unit = station['elevationUnit']
    mindate = station['mindate']
    maxdate = station['maxdate']
    datacoverage = station['datacoverage']
    
    sql = f"INSERT INTO dim_stations (station_id, dataset_id, city_id, name, latitude, longitude, elevation, elevation_unit, mindate, maxdate, datacoverage) " \
          f"VALUES ('{station_id}', '{dataset_id}', '{city_id}', '{name}', {latitude}, {longitude}, {elevation}, '{elevation_unit}', '{mindate}', '{maxdate}', {datacoverage:.4f});"
    return sql

# Fetch all city locations
cities_data = fetch_cities(dataset_id=DATA_SET_ID, location_category_id='CITY')

# Define criteria for filtering
city_names = ["Madrid", "Valencia", "Barcelona"]
max_date = "2024-08-08"

# Filter the cities
if cities_data:
    cities = cities_data.get('results', [])
    filtered_cities = filter_cities(cities, city_names, max_date)

    # Generate SQL insert statements for cities
    sql_inserts_cities = generate_sql_inserts_cities(filtered_cities, dataset_id=DATA_SET_ID)
    
    # Save the SQL insert statements for cities to a .sql file
    with open('insert_dim_locations.sql', 'w') as sql_file:
        sql_file.write("-- Switch to the 'noaa' database\n\\c noaa\n\n")
        for sql in sql_inserts_cities:
            sql_file.write(sql + '\n')
        print("SQL insert statements for cities saved to 'insert_dim_locations.sql'")
    
    # Generate and save SQL insert statements for stations
    sql_inserts_stations = []
    for city in filtered_cities:
        city_id = city['id']
        stations_data = fetch_stations(dataset_id=DATA_SET_ID, city_id=city_id)
        if stations_data and 'results' in stations_data:
            stations = stations_data['results']
            selected_station = select_station(stations, max_date.split('-')[0] + "-" + max_date.split('-')[1])
            if selected_station:
                sql_insert_station = generate_sql_inserts_stations(selected_station, city_id.split(":")[1], dataset_id=DATA_SET_ID)
                sql_inserts_stations.append(sql_insert_station)

    # Save the SQL insert statements for stations to a .sql file
    with open('insert_dim_stations.sql', 'w') as sql_file:
        sql_file.write("-- Switch to the 'noaa' database\n\\c noaa\n\n")
        for sql in sql_inserts_stations:
            sql_file.write(sql + '\n')
        print("SQL insert statements for stations saved to 'insert_dim_stations.sql'")
else:
    print("No data found.")

# Fetch all data types
data_types_data = fetch_data_types(dataset_id=DATA_SET_ID)
# Generate SQL insert statements for data types
if data_types_data and 'results' in data_types_data:
    sql_inserts_data_types = generate_sql_inserts_data_types(data_types_data['results'])

    # Save the SQL insert statements for data types to a .sql file
    with open('insert_dim_datatypes.sql', 'w') as sql_file:
        sql_file.write("-- Switch to the 'noaa' database\n\\c noaa\n\n")
        for sql in sql_inserts_data_types:
            sql_file.write(sql + '\n')
        print("SQL insert statements for data types saved to 'insert_dim_datatypes.sql'")
else:
    print("No data types found.")

# Generate the SQL insert statement
sql_insert = f"INSERT INTO dim_datasets (dataset_id, description) VALUES ('{DATA_SET_ID}', 'Daily Summaries');"

# Save the SQL insert statement to a file
with open('insert_dim_datasets.sql', 'w') as sql_file:
    sql_file.write("-- Switch to the 'noaa' database\n\\c noaa\n\n")
    sql_file.write(sql_insert)
    print("SQL insert statements for data types saved to 'insert_dim_datasets.sql'")
