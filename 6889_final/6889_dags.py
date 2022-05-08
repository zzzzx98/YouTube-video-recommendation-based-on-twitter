from datetime import datetime, timedelta
from textwrap import dedent
import time

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'zzx',
    'depends_on_past': False,
    'email': ['zz2870@columbia.edu'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),

}
with DAG(
        '6889final',
        default_args=default_args,
        description='streaming',
        #schedule_interval="00 12 * * *",
        schedule_interval=timedelta(minutes=2)
        start_date=datetime(2022, 5, 8),
        catchup=False,
        tags=['example'],
) as dag:
    # t* examples of tasks created by instantiating operators

    t1 = BashOperator(
        task_id='streaming',
        bash_command='python /home/g741150750/sparkstream.py ',
    )
    t2 = BashOperator(
        task_id='UpdateModel',
        bash_command='python /home/g741150750/twitter-LDA.py ',
    )

    t1 >> t2
