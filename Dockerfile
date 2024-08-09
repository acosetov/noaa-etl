FROM apache/airflow:2.9.0

USER root
RUN pip install boto3
USER airflow