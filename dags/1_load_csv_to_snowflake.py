from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 3, 1),
}

with DAG(
    '1_load_csv_to_snowflake',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    upload_to_stage = SnowflakeOperator(
        task_id='upload_to_stage',
        snowflake_conn_id='snowflake_conn',
        sql="PUT 'file:///opt/airflow/dags/Airline_Dataset.csv' @STAGE_1_RAW.MY_INTERNAL_STAGE AUTO_COMPRESS=TRUE;"
    )

    copy_to_table = SnowflakeOperator(
        task_id='copy_to_table',
        snowflake_conn_id='snowflake_conn',
        sql="""
        COPY INTO STAGE_1_RAW.RAW_AIRLINE_DATA (
            PASSENGER_ID, FIRST_NAME, LAST_NAME, GENDER, AGE, NATIONALITY, 
            AIRPORT_NAME, AIRPORT_COUNTRY_CODE, COUNTRY_NAME, AIRPORT_CONTINENT, 
            CONTINENTS, DEPARTURE_DATE, ARRIVAL_AIRPORT, PILOT_NAME, FLIGHT_STATUS, 
            TICKET_TYPE, PASSENGER_STATUS
        )
        FROM (
            SELECT $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18
            FROM @STAGE_1_RAW.MY_INTERNAL_STAGE/Airline_Dataset.csv.gz
        )
        FILE_FORMAT = (
            TYPE = 'CSV' 
            SKIP_HEADER = 1 
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
        )
        FORCE = TRUE
        ON_ERROR = 'CONTINUE';
        """
    )

    load_stage_2 = SnowflakeOperator(
        task_id='load_stage_2',
        snowflake_conn_id='snowflake_conn',
        sql="CALL AIRLINE_DWH.STAGE_2_INTEGRATION.LOAD_STAGE_2();"
    )

    load_stage_3 = SnowflakeOperator(
        task_id='load_stage_3',
        snowflake_conn_id='snowflake_conn',
        sql="CALL AIRLINE_DWH.STAGE_3_CORE.LOAD_STAGE_3();",
        autocommit=True
    )

    upload_to_stage >> copy_to_table >> load_stage_2 >> load_stage_3