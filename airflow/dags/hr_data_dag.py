from datetime import datetime
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

#from main import process_hr_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 10, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'schedule_interval': '@daily',
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

dag = DAG('hr_data_pipeline', 'Loads HR Data from CSV files to tables' ,default_args=default_args)


# PythonOperator(dag=dag,
#                task_id='load_hr_data',
#                provide_context=False,
#                python_callable=process_hr_data
#                )

start = BashOperator(
    task_id='notify_start',
    bash_command='echo "Starting workflow"',
    dag=dag
)

load = BashOperator(
    task_id='load_hr_data',
    bash_command='python3 /Users/akhilavudatha/workspace/Auth0/py_scripts/main.py',
    dag=dag)

stop= BashOperator(
    task_id='notify_stop',
    bash_command='echo "Ending workflow"',
    dag=dag
)

start >> load
load >> stop
