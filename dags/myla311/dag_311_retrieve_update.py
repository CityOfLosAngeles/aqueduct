# make sure to install these packages before running:
# pip install pandas
# pip install sodapy

import airflow
import logging
from sqlalchemy import create_engine, MetaData
import pandas as pd
from sodapy import Socrata
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.base_hook import BaseHook 
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
import csv
import os
import datetime

# The folllowing variables need to be setup airflow's webserver UI: Admin -> Variables
#   MY_APP_TOKEN, USERNAME, PASSWORD 

filename = '/tmp/myla311.csv'

MY_APP_TOKEN = Variable.get('MY_APP_TOKEN')
USERNAME = Variable.get('SOCRATA_USERNAME')
PASSWORD = Variable.get('SOCRATA_PASSWORD')


def retrieve_save_data(**kwargs):
    # Using Authenticated Client:
    # API Document: https://dev.socrata.com/foundry/data.lacity.org/aub4-z9pc

    client = Socrata("data.lacity.org",
                     MY_APP_TOKEN,
                     USERNAME,
                     PASSWORD)
    # Getting the total number of rows in the dataset 
    # row_count = client.get("aub4-z9pc", select="count(srnumber)")
    # row_count = pd.DataFrame.from_records(row_count)
    # row_count = row_count.count_srnumber[0]
    
    # Grabs last 10K records for upserting 
    row_count = 10000

    logging.info("The number of rows is read successfully. Now it's pulling data.")

    # Retrieving dataset using API, limit=row_count is to specify we are retrieving all the rows
    dataset = client.get("aub4-z9pc", content_type="csv", limit=row_count)
    
    logging.info("the dataset is pulled successfully.")

    # Remove prev file if exist
    if os.path.exists(filename):
        logging.info(filename + " already exist. Overwriting it")
        os.remove(filename)

    # Writing new file
    with open(filename, 'w') as writeFile:
        writer = csv.writer(writeFile)
        writer.writerows(dataset)
    
    writeFile.close()

    logging.info("the dataset is saved successfully, locally at {}.".format(filename))




# sql commands
sql_create_main = \
    """
    CREATE TABLE IF NOT EXISTS myla311_main
    (
        actiontaken text,
        address text,
        addressverified text,
        anonymous text,
        apc text,
        approximateaddress text,
        assignto text,
        cd text,
        cdmember text,
        closeddate text,
        createdbyuserorganization text,
        createddate text,
        direction text,
        housenumber text,
        latitude text,
        location text,
        location_address text,
        location_city text,
        location_state text,
        location_zip text,
        longitude text,
        mobileos text,
        nc text,
        ncname text,
        owner text,
        policeprecinct text,
        reasoncode text,
        requestsource text,
        requesttype text,
        resolutioncode text,
        servicedate text,
        srnumber text,
        status text,
        streetname text,
        suffix text,
        tbmcolumn text,
        tbmpage text,
        tbmrow text,
        updateddate text,
        zipcode text,
        PRIMARY KEY (srnumber)
    );
    """

sql_create_staging = \
    """
    DROP TABLE IF EXISTS myla311_staging;
    CREATE TABLE IF NOT EXISTS myla311_staging
    (
        actiontaken text,
        address text,
        addressverified text,
        anonymous text,
        apc text,
        approximateaddress text,
        assignto text,
        cd text,
        cdmember text,
        closeddate text,
        createdbyuserorganization text,
        createddate text,
        direction text,
        housenumber text,
        latitude text,
        location text,
        location_address text,
        location_city text,
        location_state text,
        location_zip text,
        longitude text,
        mobileos text,
        nc text,
        ncname text,
        owner text,
        policeprecinct text,
        reasoncode text,
        requestsource text,
        requesttype text,
        resolutioncode text,
        servicedate text,
        srnumber text,
        status text,
        streetname text,
        suffix text,
        tbmcolumn text,
        tbmpage text,
        tbmrow text,
        updateddate text,
        zipcode text,
        PRIMARY KEY (srnumber)
    );
    """

def insert_into_staging_table(**kwargs):
    """
    reads teh temp file and inserts into postgres using 
    python for better error handling. 
    """
    # connect to db
    connection_id = 'postgres_default' #this is the name in airflow variables
    dbhook = PostgresHook(postgres_conn_id=connection_id)
    pg_conn = dbhook.get_connection(conn_id=connection_id) 
    db_url = 'postgresql://{c.login}:{c.password}@{c.host}:{c.port}/{c.schema}'.format(c=pg_conn)
    engine = create_engine(db_url)
    meta = MetaData()
    meta.bind = engine
    logging.info("got db connection")
    # read data
    df = pd.read_csv(filename)
    # write to db. name (myla311_staging) is the table in the schema
    df.to_sql(name='myla311_staging',schema='public',con=meta.bind, if_exists='append',index=False)
    return "done"

sql_upsert = \
    """
    INSERT INTO public.myla311_staging
    SELECT * FROM public.myla311_main
    ON CONFLICT DO NOTHING;
    """

sql_delete_relects = \
    """
    DROP TABLE myla311_main_old;
    """

sql_rename_staging_to_main = \
    """
    ALTER TABLE myla311_main
    RENAME TO myla311_main_old;
    
    ALTER TABLE myla311_staging
    RENAME TO myla311_main;
    """

# airflow DAG arguments
args = {
    'owner': 'hunterowens',
    'start_date': datetime.datetime(2018, 10, 26),
    'provide_context': True,
    'email': ['hunter.owens@lacity.org'],
    'email_on_failure': False,
    'retries': 1, 
    'retry_delay': timedelta(minutes=5)
    }

# initiating the DAG
dag = airflow.DAG(
    'retrieve_update_from_myla311',
    schedule_interval="@daily",
    default_args=args,
    max_active_runs=1)


# creating tasks
task0 = PostgresOperator(
    task_id='create_main_table_if_not_exist',
    sql=sql_create_main,
    postgres_conn_id='postgres_default',
    dag=dag
    )

task1 = PostgresOperator(
    task_id='create_staging_table',
    sql=sql_create_staging,
    postgres_conn_id='postgres_default',
    dag=dag
    )

task2 = PythonOperator(
    task_id='retrieve_save_data',
    provide_context=True,
    python_callable=retrieve_save_data,
    dag=dag
    )

task3 = PythonOperator(
    task_id='insert_into_staging',
    provide_context=True,
    python_callable=insert_into_staging_table,
    dag=dag
)

task4 = PostgresOperator(
    task_id='upsert',
    sql=sql_upsert,
    postgres_conn_id='postgres_default',
    dag=dag
    )

task5 = PostgresOperator(
    task_id='sql_rename_staging_to_main',
    sql=sql_rename_staging_to_main,
    postgres_conn_id='postgres_default',
    dag=dag
)

task6 = PostgresOperator(
    task_id='delete_relects',
    sql=sql_delete_relects,
    postgres_conn_id='postgres_default',
    dag=dag
    )

# task sequence
task0 >> task1 >> task2 >> task3 >> task4 >> task5 >> task6
