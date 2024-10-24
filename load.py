from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from datetime import datetime

import snowflake.connector

def return_snowflake_conn():
    user_id = Variable.get('snowflake_username')
    password = Variable.get('snowflake_password')
    account = Variable.get('snowflake_account')

    conn = snowflake.connector.connect(
        user=user_id,
        password=password,
        account=account,
        warehouse='compute_wh',
        database='dev',
        schema='raw_data'
    )
    return conn.cursor()

@task
def set_stage():
    cur = return_snowflake_conn()
    try:
        cur.execute("BEGIN;")
        cur.execute("""CREATE OR REPLACE STAGE dev.raw_data.blob_stage
                    url = 's3://s3-geospatial/readonly/'
                    file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"');""")
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(e)
        raise e

@task
def load():
    cur = return_snowflake_conn()
    try:
        cur.execute("BEGIN;")
        cur.execute("""COPY INTO dev.raw_data.user_session_channel
                    FROM @dev.raw_data.blob_stage/user_session_channel.csv""")
        cur.execute("""COPY INTO dev.raw_data.session_timestamp
                    FROM @dev.raw_data.blob_stage/session_timestamp.csv""")
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(e)
        raise e

with DAG(
    dag_id='LOAD_SET_STAGE_DAG',
    start_date=datetime(2024, 10, 22),
    catchup=False,
    tags=['ETL'],
    schedule_interval='30 2 * * *'  # Set to run daily at 2:30 AM
) as dag:
    
    set_stage_task = set_stage()
    load_task = load()

    set_stage_task >> load_task

