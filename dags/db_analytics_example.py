import os
from datetime import datetime, timedelta
from multiprocessing.connection import Connection

import pandas as pd
import psycopg2
import psycopg2.extras as extras
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator


def insert_values(conn, table_name, templates_dict, **kwargs):
    """
    Insert data from a JSON (NEW LINE DELIMITER) to a postgres table (connection)

    :param conn: Connection to the postgres database
    :type conn: psycopg2.Connection
    :param table_name: Name of the table where insert data (table need to be created before loading data)
    :type table_name: str
    :param path_to_data: Path to the JSON file containing data to load
    :type path_to_data: str

    :return: This function return nothing, data should appears in DB table given in paramter
    """
    # Candidate have to implement this function
    pass


default_args = {
    "owner": "candidate",
    "depends_on_past": False,
    "start_date": datetime(2022, 2, 28),
    "email": ["data+airflow@meilleursagents.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retry_delay": timedelta(minutes=1),
    "priority_weight": 10000,
    "retries": 10,
}

# Define postgres connection to 
conn = psycopg2.connect(database="ma_db", user='ma_user', password='ma_password', host='db-analytics', port='5432')
# conn = Connection.get
table_name = "pet"
params={
    "table_name": table_name
}

# Get the current path
base_dir = os.path.dirname(__file__)
path_to_data = os.path.join(base_dir, 'data')

# Set dag
main_dag = DAG(
    'db_analytics_example',
    description="Example dag about how to make calculation on data in postgres db",
    schedule_interval="@once",
    default_args=default_args,
)

## Set tasks

# Start task, use LatestOnlyOperator to not perform health on previous date
start_task = DummyOperator(
    task_id="start_task",
    dag=main_dag
)

# Create a table
create_raw_table = PostgresOperator(
    dag=main_dag,
    task_id="create_raw_table",
    sql="sql/create_pet_table.sql",
    # !! Connection is created in the docker-compose file, see line which contains `AIRFLOW_CONN_LOCAL_DB_ANALYTICS`
    postgres_conn_id="local_db_analytics",
    params=params,
)

delete_partition = PostgresOperator(
    dag=main_dag,
    task_id=f'delete_{table_name}_partition',
    sql=f"DELETE FROM {table_name}" + " WHERE ds = '{{ ds }}';",
    # !! Connection is created in the docker-compose file, see line which contains `AIRFLOW_CONN_LOCAL_DB_ANALYTICS`
    postgres_conn_id="local_db_analytics",
)

# Insert data
# Candidate need to implement the function `insert_values`
insert_raw_data = PythonOperator(
    dag=main_dag,
    task_id='insert_data',
    python_callable= insert_values,
    op_kwargs = {
        "conn" : conn,
        "table_name": table_name,
    },
    templates_dict = {
        "path_to_data": os.path.join(path_to_data, table_name, f'{table_name}') + "_{{ ds_nodash }}.json",
    },
)

# Drop table
drop_stats_table = PostgresOperator(
    dag=main_dag,
    task_id="drop_stats_table",
    sql="DROP TABLE IF EXISTS owner_stats;",
    # !! Connection is created in the docker-compose file, see line which contains `AIRFLOW_CONN_LOCAL_DB_ANALYTICS`
    postgres_conn_id="local_db_analytics",
)

# Calculation number pets by owner
calculate_number_pets_by_owner = PostgresOperator(
    dag=main_dag,
    task_id="calculate_number_pets_by_owner",
    sql="sql/calculate_number_pets_by_owner.sql",
    # !! Connection is created in the docker-compose file, see line which contains `AIRFLOW_CONN_LOCAL_DB_ANALYTICS`
    postgres_conn_id="local_db_analytics",
    params=params,
)

# End task
end_task = DummyOperator(
    task_id="end_task",
    dag=main_dag
)

# Define dependencies
start_task >> create_raw_table >> delete_partition >> insert_raw_data >> drop_stats_table >> calculate_number_pets_by_owner >> end_task
