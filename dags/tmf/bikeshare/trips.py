"""
Scrape Los Angeles Metro Bikeshare trip data
"""
import os
from datetime import datetime, timedelta

import pandas
import sqlalchemy
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python_operator import PythonOperator

S3_ID = "s3_conn"
POSTGRES_ID = "postgres_default"
SCHEMA = 'tmf'
TABLE = "bike_trips"

metadata = sqlalchemy.MetaData(schema=SCHEMA)
bike_trips = sqlalchemy.Table(
    TABLE,
    metadata,
    sqlalchemy.Column('trip_id', sqlalchemy.Integer, primary_key=True, unique=True),
    sqlalchemy.Column('bike_type', sqlalchemy.String),
    sqlalchemy.Column('end_datetime', sqlalchemy.DateTime),
    sqlalchemy.Column('end_station', sqlalchemy.Integer),
    sqlalchemy.Column('end_station_name', sqlalchemy.String),
    sqlalchemy.Column('name_group', sqlalchemy.String),
    sqlalchemy.Column('optional_kiosk_id_group', sqlalchemy.String),
    sqlalchemy.Column('start_datetime', sqlalchemy.DateTime),
    sqlalchemy.Column('start_station', sqlalchemy.Integer),
    sqlalchemy.Column('start_station_name', sqlalchemy.String),
    sqlalchemy.Column('visible_id', sqlalchemy.Integer),
    sqlalchemy.Column('distance', sqlalchemy.Float),
    sqlalchemy.Column('duration', sqlalchemy.Float),
    sqlalchemy.Column('est_calories', sqlalchemy.Float),
    sqlalchemy.Column('est_carbon_offset', sqlalchemy.Float),
)


def check_columns(table, df):
    """
    Verify that a SQLAlchemy table and Pandas dataframe are compatible with
    each other. If there is a mismatch, throws an AssertionError
    """
    # A map between type names for SQLAlchemy and Pandas. This is not exhaustive.
    type_map = {
        "INTEGER": "int64",
        "VARCHAR": "object",
        "FLOAT": "float64",
        "DATETIME": "datetime64[ns]"
    }
    for column in table.columns:
        assert column.name in df.columns
        assert type_map[str(column.type)] == str(df.dtypes[column.name])


def create_table(**kwargs):
    """
    Create the schema/tables to hold the bikeshare data.
    """
    pgcon = PostgresHook.get_connection(POSTGRES_ID)
    engine = pgcon.create_sqlalchemy_engine()
    if not engine.dialect.has_schema(engine, SCHEMA):
        engine.execute(sqlalchemy.schema.CreateSchema(SCHEMA))
    metadata.create_all(engine)


def load_pg_data(**kwargs):
    pgcon = PostgresHook.get_connection(POSTGRES_ID)
    engine = pgcon.create_sqlalchemy_engine()
    check_columns(bike_trips, df)
    insert = sqlalchemy.dialects.postgresql.insert(bike_trips).on_conflict_do_nothing()
    conn = engine.connect()
    conn.execute(insert, *df.to_dict(orient="record"))


def load_s3_data(**kwargs):
    bucket = kwargs.get("bucket")
    name = kwargs.get("name", "bikeshare_trips.parquet")
    pgcon = PostgresHook.get_connection(POSTGRES_ID)
    engine = pgcon.create_sqlalchemy_engine()
    s3con = S3Hook(S3_ID)
    if bucket:
        df = pandas.read_sql_table(TABLE, engine, schema=SCHEMA)
        path = os.path.join("/tmp", name)
        ridership.to_parquet(path)
        s3con = S3Hook("s3_conn")
        s3con.load_file(path, name, bucket, replace=True)
        os.remove(path)


t1 = PythonOperator(
    task_id="scrape-ridership-data",


default_args = {
    "owner": "ianrose",
    "depends_on_past": False,
    "start_date": datetime(2019, 11, 21),
    "email": ["ian.rose@lacity.org"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(hours=1),
}

dag = DAG("bikeshare-trips", default_args=default_args, schedule_interval="@daily")

t1 = PythonOperator(
    task_id="create_table",
    provide_context=False,
    python_callable=create_table,
    op_kwargs={},
    dag=dag,
)

t2 = PythonOperator(
    task_id="load_pg_data",
    provide_context=False,
    python_callable=load_pg_data,
    op_kwargs={},
    dag=dag,
)

t3 = PythonOperator(
    task_id="load_s3_data",
    provide_context=False,
    python_callable=load_s3_data,
    op_kwargs={"bucket": "tmf-data"},
    dag=dag,
)
