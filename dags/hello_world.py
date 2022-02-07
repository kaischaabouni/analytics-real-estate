from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


def print_hello():
    return "Hello candidate !"


main_dag = DAG(
    "hello_world",
    description="Hello World DAG",
    schedule_interval="@daily",
    start_date=datetime(2021, 10, 27),
    catchup=False,
)

start_task = DummyOperator(
    task_id="start",
    dag=main_dag
)

hello = PythonOperator(
    task_id="hello_task",
    python_callable=print_hello,
    dag=main_dag
)

end_task = DummyOperator(
    task_id="end",
    dag=main_dag
)

start_task >> hello >> end_task
