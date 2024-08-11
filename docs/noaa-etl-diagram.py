from diagrams import Diagram, Cluster, Edge
from diagrams.aws.storage import S3
from diagrams.aws.compute import EC2
from diagrams.onprem.database import PostgreSQL
from diagrams.generic.compute import Rack
from diagrams.onprem.network import Internet

with Diagram("NOAA Data Pipeline Architecture", show=False):

    # External data source
    noaa_api = Internet("NOAA JSON API")

    # ETL tool
    with Cluster("ETL Process"):
        airflow = Rack("Apache Airflow")

    # Storage
    with Cluster("Data Lake"):
        raw_data_s3 = S3("Raw Data")
        transformed_data_s3 = S3("Clean Data")

    # Database
    postgres_db = PostgreSQL("PostgreSQL Database")

    # Data flow
    noaa_api >> airflow >> raw_data_s3
    airflow >> transformed_data_s3
    transformed_data_s3 >> postgres_db