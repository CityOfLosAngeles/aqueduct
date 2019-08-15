"""
DAG for ETL Processing of Dockless Mobility Provider Data
"""
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.base_hook import BaseHook
import airflow.hooks
from airflow.models import Variable
from datetime import datetime, timedelta
from configparser import ConfigParser
import time, pytz
import mds
import mds.db
import mds.providers
from mds.api import ProviderClient
import boto3
import os
import botocore
import sqlalchemy
import logging
import json

pg_conn = BaseHook.get_connection('postgres_default') 


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 10, 30), 
    'email': ['hunter.owens@lacity.org', 'mony.patel@lacity.org', 'paul.tsan@lacity.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'concurrency': 50
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(
    dag_id = 'dockless-elt',
    default_args=default_args,
    schedule_interval='@hourly'
    )

## Util Functions 

def parse_config(path):
    """
    Helper to parse a config file at :path:, which defaults to `.config`.
    """
    path = path or os.path.join(os.getcwd(), ".config")

    if not os.path.exists(path):
        print("Could not find config file: ", path)
        exit(1)

    print("Reading config file:", path)

    config = ConfigParser()
    config.read(path)

    return config

def provider_names(providers):
    """
    Returns the names of the :providers:, separated by commas.
    """
    return ", ".join([p.provider_name for p in providers])


def filter_providers(providers, names):
    """
    Filters the list of :providers: given one or more :names:.
    """
    if names is None or len(names) == 0:
        return providers

    if isinstance(names, str):
        names = [names]

    names = [n.lower() for n in names]

    return [p for p in providers if p.provider_name.lower() in names]

def connect_aws_s3():
    """ Connect to AWS and return a boto S3 session """
    if os.environ.get('env') == 'dev':
        config = ConfigParser()
        config.read(os.path.expanduser('~/.aws/credentials'))
        aws_access_key_id = config.get('la-city', 'aws_access_key_id')
        aws_secret_access_key = config.get('la-city', 'aws_secret_access_key')
    else:
        aws_conn = BaseHook.get_connection('s3_conn').extra_dejson 
        aws_access_key_id=aws_conn['aws_access_key_id']
        aws_secret_access_key=aws_conn['aws_secret_access_key']
    session = boto3.Session(aws_access_key_id=aws_access_key_id,
                            aws_secret_access_key=aws_secret_access_key)
    s3 = session.resource('s3')
    return s3

def load_to_s3(**kwargs):
    """
    Python operator to load data to s3 
    for an operator 
    """
    # load config from s3
    s3 = connect_aws_s3()
    
    try:
        s3.Bucket('city-of-los-angeles-data-lake').download_file('dockless/.config', '/tmp/.config')
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise
    config = parse_config('/tmp/.config')
    
    logging.info("Downloaded and parsed config from S3")

    # determine the MDS version to reference
    ref = config["DEFAULT"]["ref"]
    logging.info(f"Referencing MDS @ {ref}")
    
    # download the Provider registry and filter based on params
    logging.info("Downloading provider registry...")
    registry = mds.providers.get_registry(ref)
    company = kwargs['params']['company']
    logging.info(f"Acquired registry: {provider_names(registry)}")

    # filter the registry with cli args, and configure the providers that will be used
    providers = [p.configure(config, use_id=True) for p in filter_providers(registry, [company])]

    # parse any headers from the config to a dict
    # This is to fix the Bird APP-version bug. 
    for p in providers:
        headers = getattr(p, "headers", None)
        if headers and isinstance(headers, str):
            p.headers = json.loads(headers)
    logging.info(f"set company to {company}")
    # query status changes 
    end_time = kwargs['execution_date']
    ## test is provider is jump, up hours
    if providers[0].provider_id == 'c20e08cf-8488-46a6-a66c-5d8fb827f7e0': 
        start_time = end_time - timedelta(hours=25)
    else:
        start_time = end_time - timedelta(hours=12)
    client = mds.api.ProviderClient(providers=providers, ref="master")
    status_changes = client.get_status_changes(end_time=end_time, start_time=start_time)
    
    obj = s3.Object('city-of-los-angeles-data-lake',f"dockless/data/{company}/status_changes/{kwargs['ts']}.json")
    obj.put(Body=json.dumps(status_changes[providers[0]]))
    logging.info(f"Wrote {company} status changes to s3")
    # query trips 
    trips = client.get_trips(end_time=end_time, start_time=start_time)
    obj = s3.Object('city-of-los-angeles-data-lake',f"dockless/data/{company}/trips/{kwargs['ts']}.json")
    obj.put(Body=json.dumps(trips[providers[0]]))
    logging.info(f"Wrote {company} trip to s3")
    logging.info("Connecting to DB")
    user = pg_conn.login
    password = pg_conn.get_password()
    host = pg_conn.host
    dbname = pg_conn.schema
    logging.info(f"Logging into postgres://-----:----@{host}:5432/{dbname}")
    engine = sqlalchemy.create_engine(f'postgres://{user}:{password}@{host}:5432/{dbname}')
    db = mds.db.ProviderDataLoader(engine=engine)
    logging.info("loading {company} status changes into DB")
    db.load_status_changes(source=status_changes, stage_first=5)
    logging.info("loading {company} trips into DB")

    db.load_trips(source=trips, stage_first=5)
    return True

types = """
DROP TYPE IF EXISTS vehicle_types;
DROP TYPE IF EXISTS event_types;
DROP TYPE IF EXISTS event_type_reasons; 
DROP TYPE IF EXISTS propulsion_types; 

CREATE TYPE vehicle_types AS ENUM (
    'bicycle',
    'scooter'
);

CREATE TYPE  propulsion_types AS ENUM (
    'human',
    'electric_assist',
    'electric',
    'combustion'
);

CREATE TYPE event_types AS ENUM (
    'available',
    'reserved',
    'unavailable',
    'removed'
);

CREATE TYPE event_type_reasons AS ENUM (
    'service_start',
    'maintenance_drop_off',
    'rebalance_drop_off',
    'user_drop_off',
    'user_pick_up',
    'maintenance',
    'low_battery',
    'service_end',
    'rebalance_pick_up',
    'maintenance_pick_up'
);
"""

status_changes = """

CREATE TABLE IF NOT EXISTS status_changes (
    id SERIAL PRIMARY KEY 
    provider_id UUID NOT NULL,
    provider_name TEXT NOT NULL,
    device_id UUID NOT NULL,
    vehicle_id TEXT NOT NULL,
    vehicle_type vehicle_types NOT NULL,
    propulsion_type propulsion_types[] NOT NULL,
    event_type event_types NOT NULL,
    event_type_reason event_type_reasons NOT NULL,
    event_time timestamptz NOT NULL,
    event_location jsonb NOT NULL,
    battery_pct FLOAT,
    associated_trips UUID[]
);
ALTER TABLE status_changes DROP CONSTRAINT unique_event;
ALTER TABLE status_changes
    ADD CONSTRAINT  unique_event
    UNIQUE (provider_id,
            device_id,
            event_type,
            event_type_reason,
            event_time
);
"""

trips = """

CREATE TABLE  IF NOT EXISTS trips (
    provider_id UUID NOT NULL,
    provider_name TEXT NOT NULL,
    device_id UUID NOT NULL,
    vehicle_id TEXT NOT NULL,
    vehicle_type vehicle_types NOT NULL,
    propulsion_type propulsion_types[] NOT NULL,
    trip_id UUID NOT NULL,
    trip_duration INT NOT NULL,
    trip_distance INT NOT NULL,
    route jsonb NOT NULL,
    accuracy INT NOT NULL,
    start_time timestamptz NOT NULL,
    end_time timestamptz NOT NULL,
    parking_verification_url TEXT,
    standard_cost INT,
    actual_cost INT
);
ALTER TABLE trips DROP CONSTRAINT pk_trip;

ALTER TABLE trips
    ADD CONSTRAINT pk_trip
PRIMARY KEY (provider_id, trip_id);
"""

"""
task0 = PostgresOperator(
    task_id='create_types',
    sql=types,
    postgres_conn_id='postgres_default',
    dag=dag
    )

"""
task1 = PostgresOperator(
    task_id='create_status_changes',
    sql=status_changes,
    postgres_conn_id='postgres_default',
    dag=dag
    )

task2 = PostgresOperator(
    task_id='create_trips_table',
    sql=trips,
    postgres_conn_id='postgres_default',
    dag=dag
    )


providers = ['lyft', 'lime', 'jump', 'bird', 'wheels', 'spin', 'bolt']

task_list = []
for provider in providers:
    provider_to_s3_task = PythonOperator(
        task_id = f"loading_{provider}_data",
        provide_context=True,
        python_callable=load_to_s3,
        params={"company": provider},
        dag=dag)
    provider_to_s3_task.set_upstream(task2)

task1 >> task2 