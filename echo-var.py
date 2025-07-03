from airflow.operators.bash import BashOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow import DAG
import re
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'a_echo_variable',
    schedule="0-59/5 * * * *",
    default_args=default_args,
    description='A DAG to echo an ENV variable',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example","nem2p","bash"]
) as dag:

    echo_var = BashOperator(
        task_id='echo_var',
        bash_command='echo "The value of fname is: {{ var.value.FNAME }}"',
    )

    echo_var
