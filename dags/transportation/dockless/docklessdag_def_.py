"""
DAG for ETL Processing of Dockless Mobility Provider Data
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from datetime import datetime, timedelta
import time, pytz
import data_import
import data_load
import make_tables
import clear_data

pg_conn = BaseHook.get_connection('postgres_default') 
aws_conn = BaseHook.get_connection('aws_default').extra_dejson 

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 10, 9), 
    'email': ['hunter.owens@lacity.org', 'timothy.black@lacity.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=15)
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(
    dag_id = 'dockless-elt',
    default_args=default_args,
    schedule_interval='@daily'
    )

types = """
CREATE TYPE IF NOT EXITS vehicle_types AS ENUM (
    'bicycle',
    'scooter'
);

CREATE TYPE IF NOT EXITS  propulsion_types AS ENUM (
    'human',
    'electric_assist',
    'electric',
    'combustion'
);

CREATE TYPE IF NOT EXISTS event_types AS ENUM (
    'available',
    'reserved',
    'unavailable',
    'removed'
);

CREATE TYPE IF NOT EXISTS event_type_reasons AS ENUM (
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
    provider_id UUID NOT NULL,
    provider_name TEXT NOT NULL,
    device_id UUID NOT NULL,
    vehicle_id TEXT NOT NULL,
    vehicle_type vehicle_types NOT NULL,
    propulsion_type propulsion_types[] NOT NULL,
    event_type event_types NOT NULL,
    event_type_reason event_type_reasons NOT NULL,
    event_time timestamptz NOT NULL,
    event_location JSON NOT NULL,
    battery_pct FLOAT,
    associated_trips UUID[]
);

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
    route JSON NOT NULL,
    accuracy INT NOT NULL,
    start_time timestamptz NOT NULL,
    end_time timestamptz NOT NULL,
    parking_verification_url TEXT,
    standard_cost INT,
    actual_cost INT
);

ALTER TABLE trips
    ADD CONSTRAINT pk_trip
PRIMARY KEY (provider_id, trip_id);
"""

task0 = PostgresOperator(
    task_id='create_types',
    sql=types,
    postgres_conn_id='postgres_default',
    dag=dag
    )
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

# Task 1: Create tables if not exists
t1 = PythonOperator(
    task_id = 'make_provider_tables',
    python_callable = make_tables.make_tables,
    dag = dag
    )

providers = ['lemon']
feeds = ['trips'] # Add 'status_changes' when ready

# Create task for each provider / feed
for provider in providers:
    for feed in feeds:

        # Task 2: Get provider data
        t2 = PythonOperator(
            task_id = 'e_{}_{}'.format(provider, feed),
            provide_context = True, 
            python_callable = data_import.get_provider_data,
            op_kwargs = {
                'provider_name': provider,
                'feed': feed},
            dag = dag
            )

        # Task 3: Clear provider data for time period
        t3 = PythonOperator(
            task_id = 'clear_{}_{}'.format(provider, feed),
            provide_context = True, 
            python_callable = clear_data.clear_data,
            op_kwargs = {
                'provider_name': provider,
                'feed': feed},
            dag = dag
            )

        # Task 4: Upload provider data to db
        t4 = PythonOperator(
            task_id = 'tl_{}_{}'.format(provider, feed),
            provide_context = True,
            python_callable = data_load.load_json,
            op_kwargs = {
                'provider_name': provider,
                'feed': feed},
            dag = dag)

        t1 >> t2 >> t3 >> t4
