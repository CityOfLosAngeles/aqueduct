# make sure to install these packages before running:
# pip install pandas
# pip install sodapy

import airflow
import logging
import pandas as pd
from sodapy import Socrata
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.base_hook import BaseHook
from sqlalchemy import create_engine
import psycopg2 
import io
from config import MY_APP_TOKEN, USERNAME, PASSWORD, CONNECTION_ID

def get_engine(conn_id):
    # a helper function to get sqlalchemy engine from airflow hook
    connection = BaseHook.get_connection(conn_id)
    connection_uri = '{c.conn_type}://{c.login}:{c.password}@{c.host}:{c.port}/{c.schema}'.format(c=connection)
    logging.info("connection_uri: " + connection_uri)
    return create_engine(connection_uri)

def retrieve_data(**kwargs):
    # TODO: split the function into two tasks - 1.retrieval and save locally 2.replace old table

    """
        1. retrieve the dataset of 311 call center using API
        2. truncate the data table locally
        3. copy the new dataset to local data table
    """


    # Using Authenticated Client:
    # API Document: https://dev.socrata.com/foundry/data.lacity.org/aub4-z9pc

    client = Socrata("data.lacity.org",
                     MY_APP_TOKEN,
                     USERNAME,
                     PASSWORD)
    # Getting the total number of rows in the dataset 
    row_count = client.get("aub4-z9pc", select="count(createddate)")
    row_count = pd.DataFrame.from_records(row_count)
    row_count = row_count.count_createddate[0]
    
    # row_count = 1 

    logging.info("row_count is read successfully.")

    # Retrieving dataset using API, limit=row_count is to specify we are retrieving all the rows
    dataset = client.get("aub4-z9pc", limit=row_count)

    logging.info("the dataset is pulled successfully.")
    
    # Convert to pandas DataFrame
    dataset = pd.DataFrame.from_records(dataset)
    
    # get the sqlalchemy engine
    engine = get_engine(CONNECTION_ID)

    # truncates the table                                        
    dataset.head(0).to_sql('table_311', engine, if_exists='replace', index=False) 

    # copying the new dataset to the table
    conn = engine.raw_connection()
    cur = conn.cursor()
    output = io.StringIO()
    dataset.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    contents = output.getvalue()
    cur.copy_from(output, 'table_311', null="") # null values become ''
    conn.commit()
    return "Success"

# airflow DAG arguments
args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(7),
    'provide_context': True,
    # 'retries': 2, # will be set after testing
    'retry_delay': timedelta(minutes=5)
    }

# initiating the DAG
dag = airflow.DAG(
    'a_retrieve_update_from_311_two_tasks',
    schedule_interval="@daily",
    default_args=args,
    max_active_runs=1)

# creating task1
task1 = PythonOperator(
    task_id='retrieve_data_from_311',
    provide_context=True,
    python_callable=retrieve_data,
    dag=dag)





