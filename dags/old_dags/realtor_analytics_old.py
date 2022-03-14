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



def insert_realtor_values(conn, table, templates_dict, **kwargs):   

    print ("_______________________________________________BEGIN_______________________________________") 
    print(templates_dict["var_ds"])
    cur = conn.cursor()
    df_realtors=pd.read_json(templates_dict["path_to_data"],lines=True)

    for index, row in df_realtors.iterrows():
        cur.execute("INSERT INTO realtor  (realtor_id , ds, realtor_name ,city_name )  VALUES (%s, %s, %s, %s)", (row["realtor_id"] , row["ds"] , row["realtor_name"] , row["city_name"] ) )


    conn.commit()
    cur.close()
    print ("_______________________________________________END_______________________________________")


default_args = {
    "owner": "candidate",
    "depends_on_past": False,
    "start_date": datetime(2022, 1, 1), 
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
table_realtor = "realtor"
table_agent = "agent"
table_event = "event"
table_listing = "listing"
table_past_sale = "past_sale"
table_review = "review"

json_dir_realtor = "realtor"
json_dir_agent = "realtor_agent"
json_dir_event = "realtor_agent_event"
json_dir_listing = "realtor_listing"
json_dir_past_sale = "realtor_past_sale"
json_dir_review = "realtor_review"

params={
    "table_realtor": table_realtor,
    "table_agent": table_agent,
    "table_event": table_event,
    "table_listing": table_listing,
    "table_past_sale": table_past_sale,
    "table_review": table_review
}

# Get the current path
base_dir = os.path.dirname(__file__)
path_to_data = os.path.join(base_dir, 'data')

# Set dag
main_dag = DAG(
    'realtor_analytics_old',
    description="Dag realizing import raw data from json export to ",
    schedule_interval="@daily",
    default_args=default_args,
)

## Set tasks

# Start task, use LatestOnlyOperator to not perform health on previous date
start_task = DummyOperator(
    task_id="start_task",
    dag=main_dag
)

# Create a table
create_realtor_table = PostgresOperator( 
    dag=main_dag,
    task_id=f"create_{table_realtor}_table",
    sql=f"sql/realtor_tables_creation/create_{table_realtor}_table.sql",
    # !! Connection is created in the docker-compose file, see line which contains `AIRFLOW_CONN_LOCAL_DB_ANALYTICS`
    postgres_conn_id="local_db_analytics",
    params=params,
)

delete_realtor_partition = PostgresOperator(
    dag=main_dag,
    task_id=f'delete_{table_realtor}_partition',
    sql=f"DELETE FROM {table_realtor}" + " WHERE ds = '{{ ds }}';",
    # !! Connection is created in the docker-compose file, see line which contains `
    # `
    postgres_conn_id="local_db_analytics",
)

# Insert data
# Candidate need to implement the function `insert_realtor_values`
insert_realtor_data = PythonOperator(
    dag=main_dag,
    task_id=f'insert_{table_realtor}_data',
    python_callable= insert_realtor_values,
    op_kwargs = {
        "conn" : conn,
        "table": table_realtor,
    },
    templates_dict = {
        "path_to_data": os.path.join(path_to_data, json_dir_realtor, f'{json_dir_realtor}') + "_{{ ds_nodash }}.json",
        "var_ds" : "ds= '{{ ds }}'"

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


create_agent_table = PostgresOperator(
    dag=main_dag,
    task_id=f"create_{table_agent}_table",
    sql=f"sql/realtor_tables_creation/create_{table_agent}_table.sql",
    # !! Connection is created in the docker-compose file, see line which contains `AIRFLOW_CONN_LOCAL_DB_ANALYTICS`
    postgres_conn_id="local_db_analytics",
    params=params,
)

create_event_table = PostgresOperator(
    dag=main_dag,
    task_id=f"create_{table_event}_table",
    sql=f"sql/realtor_tables_creation/create_{table_event}_table.sql",
    # !! Connection is created in the docker-compose file, see line which contains `AIRFLOW_CONN_LOCAL_DB_ANALYTICS`
    postgres_conn_id="local_db_analytics",
    params=params,
)

create_listing_table = PostgresOperator(
    dag=main_dag,
    task_id=f"create_{table_listing}_table",
    sql=f"sql/realtor_tables_creation/create_{table_listing}_table.sql",
    # !! Connection is created in the docker-compose file, see line which contains `AIRFLOW_CONN_LOCAL_DB_ANALYTICS`
    postgres_conn_id="local_db_analytics",
    params=params,
)

create_past_sale_table = PostgresOperator(
    dag=main_dag,
    task_id=f"create_{table_past_sale}_table",
    sql=f"sql/realtor_tables_creation/create_{table_past_sale}_table.sql",
    # !! Connection is created in the docker-compose file, see line which contains `AIRFLOW_CONN_LOCAL_DB_ANALYTICS`
    postgres_conn_id="local_db_analytics",
    params=params,
)

create_review_table = PostgresOperator(
    dag=main_dag,
    task_id=f"create_{table_review}_table",
    sql=f"sql/realtor_tables_creation/create_{table_review}_table.sql",
    # !! Connection is created in the docker-compose file, see line which contains `AIRFLOW_CONN_LOCAL_DB_ANALYTICS`
    postgres_conn_id="local_db_analytics",
    params=params,
)


delete_agent_partition = PostgresOperator(
    dag=main_dag,
    task_id=f'delete_{table_agent}_partition',
    sql=f"DELETE FROM {table_agent}" + " WHERE ds = '{{ ds }}';",
    # !! Connection is created in the docker-compose file, see line which contains `
    # `
    postgres_conn_id="local_db_analytics",
)


delete_event_partition = PostgresOperator(
    dag=main_dag,
    task_id=f'delete_{table_event}_partition',
    sql=f"DELETE FROM {table_event}" + " WHERE ds = '{{ ds }}';",
    # !! Connection is created in the docker-compose file, see line which contains `
    # `
    postgres_conn_id="local_db_analytics",
)


delete_listing_partition = PostgresOperator(
    dag=main_dag,
    task_id=f'delete_{table_listing}_partition',
    sql=f"DELETE FROM {table_listing}" + " WHERE ds = '{{ ds }}';",
    # !! Connection is created in the docker-compose file, see line which contains `
    # `
    postgres_conn_id="local_db_analytics",
)


delete_past_sale_partition = PostgresOperator(
    dag=main_dag,
    task_id=f'delete_{table_past_sale}_partition',
    sql=f"DELETE FROM {table_past_sale}" + " WHERE ds = '{{ ds }}';",
    # !! Connection is created in the docker-compose file, see line which contains `
    # `
    postgres_conn_id="local_db_analytics",
)


delete_review_partition = PostgresOperator(
    dag=main_dag,
    task_id=f'delete_{table_review}_partition',
    sql=f"DELETE FROM {table_review}" + " WHERE ds = '{{ ds }}';",
    # !! Connection is created in the docker-compose file, see line which contains `
    # `
    postgres_conn_id="local_db_analytics",
)



def insert_agent_values(conn, table, templates_dict, **kwargs):    

    cur = conn.cursor()
    df_agents=pd.read_json(templates_dict["path_to_data"],lines=True)
    for index, row in df_agents.iterrows():
        cur.execute("INSERT INTO agent  (realtor_agent_id , ds, realtor_agent_name ,realtor_id, user_id, user_name, is_enabled, role, role_label )  VALUES (%s, %s, %s, %s,%s, %s, %s, %s,%s)", (    row["realtor_agent_id"] , row["ds"], row["realtor_agent_name"] ,row["realtor_id"], row["user_id"], row["user_name"], row["is_enabled"], row["role"], row["role_label"] ) )
    conn.commit()
    cur.close()


def insert_event_values(conn, table, templates_dict, **kwargs):   
    cur = conn.cursor()
    df_events=pd.read_json(templates_dict["path_to_data"],lines=True)
    for index, row in df_events.iterrows():
        cur.execute("INSERT INTO event  (event_id , ds, event_created_date ,event_page_main_category,realtor_id )  VALUES (%s, %s, %s, %s, %s)", (row["event_id"] , row["ds"], row["event_created_date"] ,row["event_page_main_category"], row["realtor_id"]  ) )
    conn.commit()
    cur.close()

    
def insert_listing_values(conn, table, templates_dict, **kwargs):   
    print ("_______________________________________________BEGIN_______________________________________") 
    print(templates_dict["var_ds"]) 
    cur = conn.cursor()
    df_listing = pd.read_json(templates_dict["path_to_data"],lines=True)
    for index, row in  df_listing.iterrows():
        cur.execute("INSERT INTO listing  (listing_id , ds, realtor_id ,created_ts, last_updated_ts, listing_name, transaction_type, start_ts, end_ts, item_type )  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (row["listing_id"] , row["ds"], row["realtor_id"] ,row["created_ts"], row["last_updated_ts"], row["listing_name"], row["transaction_type"], row["start_ts"], row["end_ts"], row["item_type"] ) )
    conn.commit()
    cur.close()
    print ("_______________________________________________END_______________________________________") 

    
def insert_past_sale_values(conn, table, templates_dict, **kwargs):   
    cur = conn.cursor()
    df_past_sales=pd.read_json(templates_dict["path_to_data"],lines=True)
    for index, row in df_past_sales.iterrows():
        cur.execute("INSERT INTO past_sale  (past_sale_id , ds, realtor_id ,created_ts, last_updated_ts, past_sale_name, sale_ts, item_type )  VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", (row["past_sale_id"] , row["ds"], row["realtor_id"] ,row["created_ts"], row["last_updated_ts"], row["past_sale_name"], row["sale_ts"], row["item_type"] ) )    
    conn.commit()
    cur.close() 

    
def insert_review_values(conn, table, templates_dict, **kwargs):    
    print ("_______________________________________________BEGIN_______________________________________") 
    print(templates_dict["var_ds"]) 
    cur = conn.cursor()
    df_reviews = pd.read_json(templates_dict["path_to_data"],lines=True)
    for index, row in  df_reviews.iterrows():
        cur.execute("INSERT INTO review  (review_id , ds, review_name ,review_ts, realtor_id, type_avis, moderation_status, realtor_recommendation )  VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", (row["review_id"] , row["ds"], row["review_name"] ,row["review_ts"], row["realtor_id"], row["type_avis"], row["moderation_status"], row["realtor_recommendation"]) )
    conn.commit()
    cur.close() 
    print ("_______________________________________________END_______________________________________")  



insert_agent_data = PythonOperator(
    dag=main_dag,
    task_id=f'insert_{table_agent}_data',
    python_callable= insert_agent_values,
    op_kwargs = {
        "conn" : conn,
        "table": table_agent,
    },
    templates_dict = {
        "path_to_data": os.path.join(path_to_data, json_dir_agent, f'{json_dir_agent}') + "_{{ ds_nodash }}.json",
        "var_ds" : "ds= '{{ ds }}'"

    },
)


insert_event_data = PythonOperator(
    dag=main_dag,
    task_id=f'insert_{table_event}_data',
    python_callable= insert_event_values ,
    op_kwargs = {
        "conn" : conn,
        "table": table_event,
    },
    templates_dict = {
        "path_to_data": os.path.join(path_to_data, json_dir_event, f'{json_dir_event}') + "_{{ ds_nodash }}.json",
        "var_ds" : "ds= '{{ ds }}'"

    },
)


insert_listing_data = PythonOperator(
    dag=main_dag,
    task_id=f'insert_{table_listing}_data',
    python_callable= insert_listing_values ,
    op_kwargs = {
        "conn" : conn,
        "table": table_listing,
    },
    templates_dict = {
        "path_to_data": os.path.join(path_to_data, json_dir_listing, f'{json_dir_listing}') + "_{{ ds_nodash }}.json",
        "var_ds" : "ds= '{{ ds }}'"

    },
)


insert_past_sale_data = PythonOperator(
    dag=main_dag,
    task_id=f'insert_{table_past_sale}_data',
    python_callable= insert_past_sale_values,
    op_kwargs = {
        "conn" : conn,
        "table": table_past_sale,
    },
    templates_dict = {
        "path_to_data": os.path.join(path_to_data, json_dir_past_sale, f'{json_dir_past_sale}') + "_{{ ds_nodash }}.json",
        "var_ds" : "ds= '{{ ds }}'"

    },
)


insert_review_data = PythonOperator(
    dag=main_dag,
    task_id=f'insert_{table_review}_data',
    python_callable= insert_review_values,
    op_kwargs = {
        "conn" : conn,
        "table": table_review,
    },
    templates_dict = {
        "path_to_data": os.path.join(path_to_data, json_dir_review, f'{json_dir_review}') + "_{{ ds_nodash }}.json",
        "var_ds" : "ds= '{{ ds }}'"

    },
)

# Define dependencies
start_task >> create_realtor_table >> delete_realtor_partition >> insert_realtor_data
insert_realtor_data >> create_agent_table >> delete_agent_partition >> insert_agent_data >> end_task
insert_realtor_data >> create_event_table >> delete_event_partition >> insert_event_data >> end_task
insert_realtor_data >> create_listing_table >> delete_listing_partition >> insert_listing_data >> end_task
insert_realtor_data >> create_past_sale_table >> delete_past_sale_partition >> insert_past_sale_data >> end_task
insert_realtor_data >> create_review_table >> delete_review_partition >> insert_review_data >> end_task


#>> drop_stats_table >> calculate_number_pets_by_owner >> end_task
