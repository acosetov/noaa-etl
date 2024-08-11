# NOAA ETL Pipeline

This project is designed to extract, transform, and load data from the NOAA (National Oceanic and Atmospheric Administration) API into a PostgreSQL database. The process involves extracting raw data in JSON format, storing it in an Amazon S3 bucket, transforming and cleaning the data, and then loading it into a PostgreSQL database for further analysis and querying.

## Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Technologies Used](#technologies-used)

## Project Overview

The ETL pipeline in this project performs the following tasks:

1. **Extract**: Fetch raw data from the NOAA JSON API.
2. **Store**: Save the raw JSON data into an S3 bucket for persistent storage.
3. **Transform**: Clean and transform the raw JSON files, converting them into Parquet format.
4. **Load**: Load the cleaned and transformed Parquet data into a PostgreSQL database.

## Architecture

The architecture of this ETL pipeline includes the following components:

1. **NOAA JSON API**: The source of raw weather data.
2. **Amazon S3**: Used to store both raw and cleaned data files.
3. **Apache Airflow**: Manages the ETL workflow, orchestrating the extraction, transformation, and loading processes.
4. **EC2 Instances**: Used for data transformation tasks.
5. **PostgreSQL**: The database where the cleaned data is loaded for further analysis.

### Architecture Diagram

![NOAA ETL Pipeline Architecture](docs/noaa_data_pipeline_architecture.png)

### Data Flow:

1. Data is extracted from the NOAA JSON API using an Airflow DAG (Directed Acyclic Graph).
2. The raw data is stored in an S3 bucket under a `raw_data` directory.
3. A transformation process cleans the data and converts it into Parquet format, then saves it back to S3 under a `clean-data` directory.
4. Finally, the cleaned Parquet data is loaded into a PostgreSQL database.

## Technologies Used

- **Python**: The main programming language used for data extraction and transformation.
- **Apache Airflow**: For orchestrating the ETL process.
- **Amazon S3**: For storing both raw and cleaned data.
- **PostgreSQL**: The database used to store the cleaned data.
- **Pandas**: Used for data cleaning and transformation.
- **PyArrow**: For working with Parquet files.
- **Boto3**: AWS SDK for Python, used to interact with Amazon S3.
- **Requests**: For making HTTP requests to the NOAA API.
- **Docker**: For containerizing the services.
- **Docker Compose**: For orchestrating multi-container Docker applications.