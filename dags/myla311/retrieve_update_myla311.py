# make sure to install these packages before running:
# pip install pandas
# pip install sodapy

import airflow
import logging
import pandas as pd
from sodapy import Socrata
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
import boto3
import botocore
import os
from config import MY_APP_TOKEN, USERNAME, PASSWORD, CONNECTION_ID


def retrieve_data(**kwargs):
    # Using Authenticated Client:
    # API Document: https://dev.socrata.com/foundry/data.lacity.org/aub4-z9pc

    client = Socrata("data.lacity.org",
                     MY_APP_TOKEN,
                     USERNAME,
                     PASSWORD)
    # Getting the total number of rows in the dataset 
    row_count = client.get("aub4-z9pc", select="count(srnumber)")
    row_count = pd.DataFrame.from_records(row_count)
    row_count = row_count.count_srnumber[0]
    
    # row_count = 1000 # for test purpose only

    logging.info("The number of rows is read successfully.")

    # Retrieving dataset using API, limit=row_count is to specify we are retrieving all the rows
    dataset = client.get("aub4-z9pc", limit=row_count)
    print(dataset)

    logging.info("the dataset is pulled successfully.")

    # Convert to pandas DataFrame
    return pd.DataFrame.from_records(dataset)


def upload_to_s3(**kwargs):
    dataset = retrieve_data()
    filename = './myla311.csv'
    dataset.to_csv(filename, index=False)
    logging.info("the dataset is saved locally as " + os.path.abspath(filename))

    # saving to s3 bucket
    bucket_name = "myla311-dev"
    key = './myla311.csv'
    output_name = "myla311.csv"

    s3 = boto3.client('s3')
    s3.upload_file(key, bucket_name, output_name)


def download_from_s3(**kwargs):
    bucket_name = "myla311-dev"
    key = "myla311.csv"
    output_name = './myla311.csv'

    s3 = boto3.resource('s3')

    try:
        s3.Bucket(bucket_name).download_file(key, output_name)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise

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

sql_insert_into_staging = \
    """
    COPY myla311_staging
    FROM '{}' WITH CSV HEADER delimiter ',';
    """.format(os.path.abspath('./myla311.csv'))

sql_rename_staging_to_main = \
    """
    ALTER TABLE myla311_main
    RENAME TO myla311_main_old;
    
    ALTER TABLE myla311_staging
    RENAME TO myla311_main;
    """

sql_delete_main_old = \
    """
    DROP TABLE myla311_main_old;
    """


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
    'retrieve_update_from_myla311',
    schedule_interval="@daily",
    default_args=args,
    max_active_runs=1)


# creating tasks
task0 = PostgresOperator(
    task_id='create_main_table_if_not_exist',
    sql=sql_create_main,
    postgres_conn_id=CONNECTION_ID,
    dag=dag
    )

task1 = PostgresOperator(
    task_id='create_staging_table',
    sql=sql_create_staging,
    postgres_conn_id=CONNECTION_ID,
    dag=dag
    )

task2 = PythonOperator(
    task_id='download_upload_to_s3',
    provide_context=True,
    python_callable=upload_to_s3,
    dag=dag
    )

task3 = PythonOperator(
    task_id='download_from_s3',
    provide_context=True,
    python_callable=download_from_s3,
    dag=dag
    )

task4 = PostgresOperator(
    task_id='insert_into_staging_table',
    sql=sql_insert_into_staging ,
    postgres_conn_id=CONNECTION_ID,
    dag=dag
    )

task5 = PostgresOperator(
    task_id='rename_staging_to_main',
    sql=sql_rename_staging_to_main,
    postgres_conn_id=CONNECTION_ID,
    dag=dag
    )

task6 = PostgresOperator(
    task_id='delete_main_old',
    sql=sql_delete_main_old,
    postgres_conn_id=CONNECTION_ID,
    dag=dag
    )

# task sequence
task0 >> task1 >> task2 >> task3 >> task4 >> task5 >> task6
