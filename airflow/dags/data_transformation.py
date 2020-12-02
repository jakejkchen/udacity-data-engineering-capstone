import os
import logging

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 1),
    'retries': 0,
    'provide_context': True
}

# Initialize the DAG
# Concurrency --> Number of tasks allowed to run concurrently
dag = DAG('spakify_etl', schedule_interval=None, default_args=default_args)

start_operator = DummyOperator(task_id='Start_execution',  dag=dag)

process_airport = BashOperator(
    task_id='process_airport',
    bash_command='python /home/workspace/airflow/dags/transform/processing_airport.py',
    dag=dag
)

process_demo = BashOperator(
    task_id='process_demo',
    bash_command='python /home/workspace/airflow/dags/transform/processing_demo.py',
    dag=dag
)

process_immigration = BashOperator(
    task_id='process_immigration',
    bash_command='python /home/workspace/airflow/dags/transform/processing_immigration.py',
    dag=dag
)

process_temp = BashOperator(
    task_id='process_temperature',
    bash_command='python /home/workspace/airflow/dags/transform/processing_temp.py',
    dag=dag
)

data_quality_check = BashOperator(
    task_id='data_quality_check',
    bash_command='python /home/workspace/airflow/dags/transform/data_quality_check.py',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# construct the DAG by setting the dependencies
start_operator >> [process_airport, process_demo, process_immigration, process_temp]
[process_airport, process_demo, process_immigration, process_temp] >> data_quality_check
data_quality_check >> end_operator

# start_operator >> process_demo >> end_operator
# start_operator >> process_immigration >> end_operator
# start_operator >> process_temp >> end_operator