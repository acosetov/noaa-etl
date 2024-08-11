-- init.sql

-- Create the 'noaa' database if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT FROM pg_database WHERE datname = 'noaa'
    ) THEN
        PERFORM dblink_exec('dbname=' || current_database(), 'CREATE DATABASE noaa');
    END IF;
END
$$;

-- Switch to the 'noaa' database
\c noaa


-- Create the dim_datasets table
CREATE TABLE IF NOT EXISTS dim_datasets (
    dataset_id VARCHAR(10) PRIMARY KEY,
    description VARCHAR(255)
);

-- Create the dim_datasets table
CREATE TABLE dim_datatypes (
    datatype_id VARCHAR(10) PRIMARY KEY,
    description VARCHAR(255) NOT NULL
);

-- Create the dim_locations table
CREATE TABLE IF NOT EXISTS dim_locations (
    city_id VARCHAR(50) PRIMARY KEY,
    dataset_id VARCHAR(10),
    mindate DATE,
    maxdate DATE,
    name VARCHAR(100),
    datacoverage DECIMAL(5, 4),
    FOREIGN KEY (dataset_id) REFERENCES dim_datasets(dataset_id)
);

-- Create the dim_stations table
CREATE TABLE IF NOT EXISTS dim_stations (
    station_id VARCHAR(11) PRIMARY KEY,
    dataset_id VARCHAR(10),
    city_id VARCHAR(50),
    name VARCHAR(100),
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    elevation DECIMAL(6,2),
    elevation_unit VARCHAR(10),
    mindate DATE,
    maxdate DATE,
    datacoverage DECIMAL(5,4),
    FOREIGN KEY (dataset_id) REFERENCES dim_datasets(dataset_id),
    FOREIGN KEY (city_id) REFERENCES dim_locations(city_id)
);

-- Create the dim_date table
CREATE TABLE IF NOT EXISTS dim_date (
    date_id SERIAL PRIMARY KEY,
    date DATE UNIQUE,
    year INT,
    month INT,
    day INT,
    day_of_week VARCHAR(10),
    week_of_year INT,
    quarter INT
);

-- Create the fact_weather table
CREATE TABLE IF NOT EXISTS fact_weather (
    fact_id SERIAL PRIMARY KEY,
    date_id INT REFERENCES dim_date(date_id),
    datatype_id VARCHAR(10) REFERENCES dim_datatypes(datatype_id),
    station_id VARCHAR(11) REFERENCES dim_stations(station_id),
    attributes VARCHAR(50),
    value DECIMAL(10, 2),
    UNIQUE (date_id, station_id, datatype_id)
);