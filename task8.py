import logging
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook  
import datetime
from airflow.operators.bash import BashOperator
import snowflake.connector
import logging
from airflow.sensors.filesystem import FileSensor
import os
import shutil


put_q = "PUT file:///home/oamin/tasks/task8/source/Airline_Dataset.csv @Internal_stage;"


def to_table_query(destination, source):
    return f"""COPY INTO {destination} FROM {source}
    FILE_FORMAT = (type = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY ='"');"""

def create_put_query():
    files = os.listdir(f"{os.getcwd()}/source/")
    query = '\n'.join([f'PUT file://{os.getcwd()}/source/{x} @Internal_stage;' for x in files])
    return query


def move():
    files = os.listdir(f"{os.getcwd()}/source/")
    os.makedirs(f'{os.getcwd()}/handled_files', exist_ok=True)
    for fle in files:
        os.replace(f'{os.getcwd()}/source/{fle}', f'{os.getcwd()}/handled_files/{fle}')


with DAG(dag_id="task_8_",
    start_date=datetime.datetime(2024, 1, 1),
    schedule_interval = "*/1 * * * *",
    catchup=False) as dag:
    
    # create_in_st = SnowflakeOperator(
    #     task_id = 'create_internal_stage',
    #     snowflake_conn_id="snowflake_connection",
    #     sql = create_stage_q
    #     )

    check_file = FileSensor(task_id="wait_for_file", filepath=f"{os.getcwd()}/source/")
    
    put_in_st = SnowflakeOperator(
        task_id = 'put_in_internal',
        snowflake_conn_id = 'snowflake_connection',
        sql = create_put_query()
    )
    # to_raw_data = SnowflakeOperator(
    #     task_id = 'to_raw',
    #     snowflake_conn_id = 'snowflake_connection',
    #     sql = to_table_query('AIRLINE', '@Internal_stage')
    # )
    move_files = PythonOperator(
        task_id = 'move_file',
        python_callable = move
    )

    check_file >> put_in_st >> move_files


    # insert_values = PythonOperator(task_id = 'insert_values', python_callable = create_fun)
    # insert_values