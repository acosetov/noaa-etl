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

-- Create the dim_station table
CREATE TABLE IF NOT EXISTS dim_station (
    station_id VARCHAR(30) PRIMARY KEY,
    station_name VARCHAR(255) 
);

-- Create the dim_datatype table
CREATE TABLE IF NOT EXISTS dim_datatype (
    datatype_id VARCHAR(10) PRIMARY KEY,
    datatype_description VARCHAR(255) 
);

-- Create the fact_weather table
CREATE TABLE IF NOT EXISTS fact_weather (
    weather_id SERIAL PRIMARY KEY,
    date_id INT,
    station_id VARCHAR(30),
    datatype_id VARCHAR(10),
    value INT,
    attributes VARCHAR(255),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (station_id) REFERENCES dim_station(station_id),
    FOREIGN KEY (datatype_id) REFERENCES dim_datatype(datatype_id),
    UNIQUE (date_id, station_id, datatype_id)
);