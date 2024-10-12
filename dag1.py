import datetime
import airflow
from airflow.operators.python_operator import PythonOperator

with airflow.DAG(
  "composer_sample_celery_kubernetes",
  start_date=datetime.datetime(2022, 1, 1),
  schedule_interval="@daily") as dag:

  def kubernetes_example():
      print("This task runs using KubernetesExecutor")

  def celery_example():
      print("This task runs using CeleryExecutor")

  # To run with KubernetesExecutor, set queue to kubernetes
  task_kubernetes = PythonOperator(
    task_id='task-kubernetes',
    python_callable=kubernetes_example,
    dag=dag,
    queue='kubernetes')

  # To run with CeleryExecutor, omit the queue argument
  task_celery = PythonOperator(
    task_id='task-celery',
    python_callable=celery_example,
    dag=dag)

  task_kubernetes >> task_celery