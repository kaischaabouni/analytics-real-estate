from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.operators.postgres_operator import PostgresOperator

default_args = {
    "owner": "candidate",
    "depends_on_past": False,
    "start_date": datetime(2022, 2, 10),
    "email": ["data+airflow@meilleursagents.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retry_delay": timedelta(minutes=1),
    "priority_weight": 10000,
    "retries": 10,
}

# Set dag
main_dag = DAG(
    'db_analytics_example',
    description="Export IWB S3 estima data from GCS to Google Cloud Storage",
    schedule_interval="@daily",
    default_args=default_args,
)

## Set tasks

# Start task, use LatestOnlyOperator to not perform health on previous date
start_task = LatestOnlyOperator(
    task_id="start_task",
    dag=main_dag
)

# Create a table
create_table = PostgresOperator(
    dag=main_dag,
    task_id="create_table",
    sql="sql/create_health_check_table.sql",
    # !! Connection is created in the docker-compose file, see line which contains `AIRFLOW_CONN_LOCAL_DB_ANALYTICS`
    postgres_conn_id="local_db_analytics",
)

# Delete row to be idempotent
delete_row = PostgresOperator(
    dag=main_dag,
    task_id="delete_row",
    sql="DELETE FROM health_check WHERE datestamp = '{{ ds }}'",
    # !! Connection is created in the docker-compose file, see line which contains `AIRFLOW_CONN_LOCAL_DB_ANALYTICS`
    postgres_conn_id="local_db_analytics",
)

# Insert a row
insert_row = PostgresOperator(
    dag=main_dag,
    task_id="insert_row",
    sql="sql/load_health_check.sql",
    # !! Connection is created in the docker-compose file, see line which contains `AIRFLOW_CONN_LOCAL_DB_ANALYTICS`
    postgres_conn_id="local_db_analytics",
)

# End task
end_task = DummyOperator(
    task_id="end_task",
    dag=main_dag
)

# Define dependencies
start_task >> create_table >> delete_row >> insert_row >> end_task
