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



def insert_values(conn, table, templates_dict, **kwargs):
    """
    Insert data from a JSON (NEW LINE DELIMITER) to a postgres table (connection)

    :param conn: Connection to the postgres database
    :type conn: psycopg2.Connection
    :param table: Name of the table where insert data (table need to be created before loading data)
    :type table: str
    :param templates_dict: Dict containing the path to the data file to load (key: path_to_data),
                            using this allow the file path to have good datestamp regarding the ds of airflow execution
    :type templates_dict: Dict

    :return: This function return nothing, data should appears in DB table given in paramter
    """
    # Candidate have to implement this function
    #pass
    print ("_______________________________________________BEGIN_______________________________________")
    print(templates_dict["var_test"])
    cur = conn.cursor()
    df_pets=pd.read_json(templates_dict["path_to_data"],lines=True)

    for index, row in df_pets.iterrows():
        cur.execute("INSERT INTO pet  (ds , name , pet_type , birth_date , owner)  VALUES (%s, %s, %s, %s, %s)", (row["ds"] , row["name"] , row["pet_type"] , row["birth_date"] , row["owner"]) )


    print ("_______________________________________________END_______________________________________")


    conn.commit()
    cur.close()


default_args = {
    "owner": "candidate",
    "depends_on_past": False,
    "start_date": datetime(2022, 3, 1),
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
table = "pet"
params={
    "table": table
}

# Get the current path
base_dir = os.path.dirname(__file__)
path_to_data = os.path.join(base_dir, 'data')

# Set dag 
main_dag = DAG(
    'db_analytics_example',
    description="Example dag about how to make calculation on data in postgres db",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False

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
    task_id=f"create_{table}_table",
    sql=f"sql/create_{table}_table.sql",
    # !! Connection is created in the docker-compose file, see line which contains `AIRFLOW_CONN_LOCAL_DB_ANALYTICS`
    postgres_conn_id="local_db_analytics",
    params=params,
)

delete_partition = PostgresOperator(
    dag=main_dag,
    task_id=f'delete_{table}_partition',
    sql=f"DELETE FROM {table}" + " WHERE ds = '{{ ds }}';",
    # !! Connection is created in the docker-compose file, see line which contains `
    # `
    postgres_conn_id="local_db_analytics",
)

# Insert data
# Candidate need to implement the function `insert_values`
insert_raw_data = PythonOperator(
    dag=main_dag,
    task_id=f'insert_{table}_data',
    python_callable= insert_values,
    op_kwargs = {
        "conn" : conn,
        "table": table,
    },
    templates_dict = {
        "path_to_data": os.path.join(path_to_data, table, f'{table}') + "_{{ ds_nodash }}.json",
        "var_test" : "hqhqhq '{{ ds }}'"
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
