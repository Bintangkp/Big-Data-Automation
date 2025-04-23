import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd

default_args = {
    'owner': 'bintang',
    'start_date': dt.datetime(2024, 11, 1),
    'retries': 0,
    'retry_delay': dt.timedelta(minutes=300),
}

with DAG('Milestone3',
         default_args=default_args,
         schedule_interval='10-30/10 9 * * 6',
         catchup=False,
         ) as dag:

    extractdata = BashOperator(task_id='extract',
                                 bash_command='sudo -u airflow python /opt/airflow/dags/extract.py')   
    
    transformdata = BashOperator(task_id='transform',
                                 bash_command='sudo -u airflow python /opt/airflow/dags/transform.py')
    loaddata = BashOperator(task_id='load',
                                 bash_command='sudo -u airflow python /opt/airflow/dags/load.py')

extractdata >> transformdata >> loaddata
