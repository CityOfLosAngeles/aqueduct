"""
Download Los Angeles Metro Bikeshare trip data from Tableau,
upload to Postgres and S3.
"""
import io
import logging
import os
from datetime import datetime, timedelta

import pandas
import sqlalchemy

import tableauserverclient
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

S3_ID = "s3_conn"
POSTGRES_ID = "postgres_default"
SCHEMA = "transportation"
TABLE = "bike_trips"


# Define the PostgreSQL Table using SQLAlchemy
metadata = sqlalchemy.MetaData(schema=SCHEMA)
bike_trips = sqlalchemy.Table(
    TABLE,
    metadata,
    sqlalchemy.Column("trip_id", sqlalchemy.Integer, primary_key=True, unique=True),
    sqlalchemy.Column("bike_type", sqlalchemy.String),
    sqlalchemy.Column("end_datetime", sqlalchemy.DateTime),
    sqlalchemy.Column("end_station", sqlalchemy.String),
    sqlalchemy.Column("end_station_name", sqlalchemy.String),
    sqlalchemy.Column("name_group", sqlalchemy.String),
    sqlalchemy.Column("optional_kiosk_id_group", sqlalchemy.String),
    sqlalchemy.Column("start_datetime", sqlalchemy.DateTime),
    sqlalchemy.Column("start_station", sqlalchemy.String),
    sqlalchemy.Column("start_station_name", sqlalchemy.String),
    sqlalchemy.Column("visible_id", sqlalchemy.String),
    sqlalchemy.Column("distance", sqlalchemy.Float),
    sqlalchemy.Column("duration", sqlalchemy.Float),
    sqlalchemy.Column("est_calories", sqlalchemy.Float),
    sqlalchemy.Column("est_carbon_offset", sqlalchemy.Float),
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
        "DATETIME": "datetime64[ns]",
    }
    for column in table.columns:
        assert column.name in df.columns
        logging.info(
            f"Checking that {column.name}'s type {column.type} "
            f"is consistent with {df.dtypes[column.name]}"
        )
        assert type_map[str(column.type)] == str(df.dtypes[column.name])


def create_table(**kwargs):
    """
    Create the schema/tables to hold the bikeshare data.
    """
    logging.info("Creating tables")
    engine = PostgresHook.get_hook(POSTGRES_ID).get_sqlalchemy_engine()
    if not engine.dialect.has_schema(engine, SCHEMA):
        engine.execute(sqlalchemy.schema.CreateSchema(SCHEMA))
    metadata.create_all(engine)


def load_pg_data(**kwargs):
    """
    Load data from the Tableau server and upload it to Postgres.
    """
    # Sign in to the tableau server.
    TABLEAU_SERVER = "https://10az.online.tableau.com"
    TABLEAU_SITENAME = "echo"
    TABLEAU_VERSION = "2.7"
    TABLEAU_USER = Variable.get("BIKESHARE_TABLEAU_USER")
    TABLEAU_PASSWORD = Variable.get("BIKESHARE_TABLEAU_PASSWORD")
    TRIP_TABLE_VIEW_ID = "7530c937-887e-42da-aa50-2a11d279bf51"
    logging.info("Authenticating with Tableau")
    tableau_auth = tableauserverclient.TableauAuth(
        TABLEAU_USER, TABLEAU_PASSWORD, TABLEAU_SITENAME,
    )
    tableau_server = tableauserverclient.Server(TABLEAU_SERVER, TABLEAU_VERSION)
    tableau_server.auth.sign_in(tableau_auth)

    # Get the Trips table view. This is a view specifically created for
    # this DAG. Tableau server doesn't allow the download of underlying
    # workbook data via the API (though one can from the UI). This view
    # allows us to get around that.
    logging.info("Loading Trips view")
    all_views, _ = tableau_server.views.get()
    view = next(v for v in all_views if v.id == TRIP_TABLE_VIEW_ID)
    if not view:
        raise Exception("Cannot find the trips table!")
    tableau_server.views.populate_csv(view)
    df = pandas.read_csv(
        io.BytesIO(b"".join(view.csv)),
        parse_dates=["Start Datetime", "End Datetime"],
        thousands=",",
        dtype={"Visible ID": str, "End Station": str, "Start Station": str},
    )

    # The data has a weird structure where trip rows are duplicated, with variations
    # on a "Measure" column, containing trip length, duration, etc. We pivot on that
    # column to create a normalized table containing one row per trip.
    logging.info("Cleaning Data")
    df = pandas.merge(
        df.set_index("Trip ID")
        .groupby(level=0)
        .first()
        .drop(columns=["Measure Names", "Measure Values"]),
        df.pivot(index="Trip ID", columns="Measure Names", values="Measure Values"),
        left_index=True,
        right_index=True,
    ).reset_index()
    df = df.rename(
        {
            n: n.lower().strip().replace(" ", "_").replace("(", "").replace(")", "")
            for n in df.columns
        },
        axis="columns",
    )
    check_columns(bike_trips, df)

    # Upload the final dataframe to Postgres. Since pandas timestamps conform to the
    # datetime interface, psycopg can correctly handle the timestamps upon insert.
    logging.info("Uploading to PG")
    engine = PostgresHook.get_hook(POSTGRES_ID).get_sqlalchemy_engine()
    insert = sqlalchemy.dialects.postgresql.insert(bike_trips).on_conflict_do_nothing()
    conn = engine.connect()
    conn.execute(insert, *df.to_dict(orient="record"))


def load_s3_data(**kwargs):
    """
    Load the table from PG and upload it as a parquet to S3.
    """
    bucket = kwargs.get("bucket")
    name = kwargs.get("name", "bikeshare_trips.parquet")
    if bucket:
        logging.info("Uploading data to s3")
        engine = PostgresHook.get_hook(POSTGRES_ID).get_sqlalchemy_engine()
        df = pandas.read_sql_table(TABLE, engine, schema=SCHEMA)
        path = os.path.join("/tmp", name)
        df.to_parquet(path)
        s3con = S3Hook(S3_ID)
        s3con.load_file(path, name, bucket, replace=True)
        os.remove(path)


default_args = {
    "owner": "ianrose",
    "depends_on_past": False,
    "start_date": datetime(2019, 11, 21),
    "email": ["ITAData@lacity.org"],
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

t1 >> t2 >> t3
