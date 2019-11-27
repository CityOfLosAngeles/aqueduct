"""
Download LADOT Downtown DASH data, and upload it to Postgres and S3.
"""
import logging
import os
from base64 import b64encode
from datetime import datetime, timedelta

import pandas
import requests
import sqlalchemy
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

S3_ID = "s3_conn"
POSTGRES_ID = "postgres_default"
SCHEMA = "transportation"
TABLE = "dash_trips"
LOCAL_TIMEZONE = "US/Pacific"


# Define the PostgreSQL Table using SQLAlchemy
metadata = sqlalchemy.MetaData(schema=SCHEMA)
dash_trips = sqlalchemy.Table(
    TABLE,
    metadata,
    sqlalchemy.Column("trip_id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("arrival_passengers", sqlalchemy.Integer),
    sqlalchemy.Column("arrive", sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column("arrive_variance", sqlalchemy.Float),
    sqlalchemy.Column("block_href", sqlalchemy.String),
    sqlalchemy.Column("depart", sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column("depart_variance", sqlalchemy.Float),
    sqlalchemy.Column("departure_passengers", sqlalchemy.Integer),
    sqlalchemy.Column("driver_href", sqlalchemy.String),
    sqlalchemy.Column("offs", sqlalchemy.Integer),
    sqlalchemy.Column("ons", sqlalchemy.Integer),
    sqlalchemy.Column("pattern_href", sqlalchemy.String),
    sqlalchemy.Column("pattern_name", sqlalchemy.String),
    sqlalchemy.Column("route_href", sqlalchemy.String),
    sqlalchemy.Column("route_name", sqlalchemy.String),
    sqlalchemy.Column("run_href", sqlalchemy.String),
    sqlalchemy.Column("run_name", sqlalchemy.String),
    sqlalchemy.Column("scheduled_arrive", sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column("scheduled_depart", sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column("stop_href", sqlalchemy.String),
    sqlalchemy.Column("stop_name", sqlalchemy.String),
    sqlalchemy.Column("trip_href", sqlalchemy.String),
    sqlalchemy.Column("trip_name", sqlalchemy.String),
    sqlalchemy.Column("vehicle_href", sqlalchemy.String),
    sqlalchemy.Column("vehicle_name", sqlalchemy.String),
)


def get_bearer_token():
    user = Variable.get("SYNCROMATICS_USER")
    password = Variable.get("SYNCROMATICS_PASSWORD")
    login = b64encode(f"{user}:{password}".encode()).decode()

    r = requests.post(
        "https://track-api.syncromatics.com/1/login",
        headers={"Authorization": f"Basic {login}"},
    )
    r.raise_for_status()
    return r.content.decode()


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
        "DATETIME": "datetime64[ns, US/Pacific]",
    }
    for column in table.columns:
        assert column.name in df.columns
        assert type_map[str(column.type)] == str(df.dtypes[column.name])


def create_table(**kwargs):
    """
    Create the schema/tables to hold the hare data.
    """
    logging.info("Creating tables")
    engine = PostgresHook.get_hook(POSTGRES_ID).get_sqlalchemy_engine()
    if not engine.dialect.has_schema(engine, SCHEMA):
        engine.execute(sqlalchemy.schema.CreateSchema(SCHEMA))
    metadata.create_all(engine)


def load_pg_data(ds, **kwargs):
    """
    Query trips data from the Syncromatics REST API and upload it to Postgres.
    """

    # Fetch the data from the rest API for the previous day.
    DOWNTOWN_DASH_ID = "LADOTDT"
    token = get_bearer_token()
    yesterday = str(pandas.to_datetime(ds) - timedelta(1))
    logging.info(f"Fetching DASH data for {yesterday}")
    r = requests.get(
        f"https://track-api.syncromatics.com/1/{DOWNTOWN_DASH_ID}"
        f"/exports/stop_times.json?start={yesterday}&end={yesterday}",
        headers={"Authorization": f"Bearer {token}"},
    )
    time_cols = ["arrive", "depart", "scheduled_arrive", "scheduled_depart"]
    df = pandas.read_json(
        r.content,
        convert_dates=time_cols,
        dtype={
            "run_name": str,
            "vehicle_name": str,
            "arrive_variance": float,
            "depart_variance": float,
        },
    )

    # Drop unnecesary driver info.
    df = df.drop(columns=["driver_first_name", "driver_last_name"])

    # Drop null trip ids and make sure they are integers.
    df = df.dropna(subset=["trip_id"])
    df.trip_id = df.trip_id.astype("int64")

    # Set the timezone to local time with TZ info
    for col in time_cols:
        df[col] = df[col].dt.tz_localize("UTC").dt.tz_convert(LOCAL_TIMEZONE)

    check_columns(dash_trips, df)

    # Upload the final dataframe to Postgres. Since pandas timestamps conform to the
    # datetime interfaace, psycopg can correctly handle the timestamps upon insert.
    logging.info("Uploading to PG")
    engine = PostgresHook.get_hook(POSTGRES_ID).get_sqlalchemy_engine()
    insert = sqlalchemy.dialects.postgresql.insert(dash_trips).on_conflict_do_nothing()
    conn = engine.connect()
    conn.execute(insert, *df.to_dict(orient="record"))


def load_s3_data(**kwargs):
    """
    Load the table from PG and upload it as a parquet to S3.
    """
    bucket = kwargs.get("bucket")
    name = kwargs.get("name", "dash_trips.parquet")
    if bucket:
        logging.info("Uploading data to s3")
        engine = PostgresHook.get_hook(POSTGRES_ID).get_sqlalchemy_engine()
        df = pandas.read_sql_table(TABLE, engine, schema=SCHEMA)
        path = os.path.join("/tmp", name)
        # Write to parquet, allowing timestamps to be truncated to millisecond.
        # This is much more precision than we will ever need or get.
        df.to_parquet(path, allow_truncated_timestamps=True)
        s3con = S3Hook(S3_ID)
        s3con.load_file(path, name, bucket, replace=True)
        os.remove(path)


default_args = {
    "owner": "ianrose",
    "depends_on_past": False,
    "start_date": datetime(2019, 11, 25),
    "email": ["ITAData@lacity.org"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(hours=1),
}

# TODO: we may want to customize more precisely when this runs.
dag = DAG("dash-trips", default_args=default_args, schedule_interval="@daily")

t1 = PythonOperator(
    task_id="create_table",
    provide_context=False,
    python_callable=create_table,
    op_kwargs={},
    dag=dag,
)

t2 = PythonOperator(
    task_id="load_pg_data",
    provide_context=True,
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

t1 >> t2 >> t3
