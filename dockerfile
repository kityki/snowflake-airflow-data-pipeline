FROM apache/airflow:2.8.2

USER airflow

RUN pip install --no-cache-dir apache-airflow-providers-snowflake