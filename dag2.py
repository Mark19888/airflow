from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

# Define the Python functions for the tasks
def print_hello():
    print("Hello, World!")

def print_completion():
    print("Task Completed")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'sample_airflow_job',
    default_args=default_args,
    description='A simple Airflow DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Task 0: Start (Dummy Operator)
    start = DummyOperator(
        task_id='start'
    )

    # Task 1: Print Hello World
    task_1 = PythonOperator(
        task_id='print_hello',
        python_callable=print_hello
    )

    # Task 2: Print Task Completion
    task_2 = PythonOperator(
        task_id='print_completion',
        python_callable=print_completion
    )

    # Set the task dependencies
    start >> task_1 >> task_2