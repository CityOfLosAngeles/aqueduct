"""
Download Los Angeles Metro Bikeshare trip data from Tableau,
upload to Postgres and S3.
"""
import io
import os
from urllib.parse import quote_plus

import pandas
import sqlalchemy
import tableauserverclient

SCHEMA = "transportation"
TABLE = "bike_trips"
S3_DATA_PATH = "s3://tmf-ita-data/bikeshare_trips.parquet"


if os.environ.get("DEV"):
    POSTGRES_URI = os.environ.get("POSTGRES_URI")
else:
    POSTGRES_URI = (
        f"postgres://"
        f"{quote_plus(os.environ['POSTGRES_CREDENTIAL_USERNAME'])}:"
        f"{quote_plus(os.environ['POSTGRES_CREDENTIAL_PASSWORD'])}@"
        f"{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}"
        f"/{os.environ['POSTGRES_DATABASE']}"
    )
engine = sqlalchemy.create_engine(POSTGRES_URI)


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
        print(
            f"Checking that {column.name}'s type {column.type} "
            f"is consistent with {df.dtypes[column.name]}"
        )
        assert type_map[str(column.type)] == str(df.dtypes[column.name])


def create_table(**kwargs):
    """
    Create the schema/tables to hold the bikeshare data.
    """
    print("Creating tables")
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
    TABLEAU_USER = os.environ.get("BIKESHARE_USERNAME")
    TABLEAU_PASSWORD = os.environ.get("BIKESHARE_PASSWORD")
    TRIP_TABLE_VIEW_ID = "7530c937-887e-42da-aa50-2a11d279bf51"
    print("Authenticating with Tableau")
    tableau_auth = tableauserverclient.TableauAuth(
        TABLEAU_USER, TABLEAU_PASSWORD, TABLEAU_SITENAME,
    )
    tableau_server = tableauserverclient.Server(TABLEAU_SERVER, TABLEAU_VERSION)
    tableau_server.auth.sign_in(tableau_auth)

    # Get the Trips table view. This is a view specifically created for
    # this DAG. Tableau server doesn't allow the download of underlying
    # workbook data via the API (though one can from the UI). This view
    # allows us to get around that.
    print("Loading Trips view")
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
    print("Cleaning Data")
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
    print("Uploading to PG")
    insert = sqlalchemy.dialects.postgresql.insert(bike_trips).on_conflict_do_nothing()
    conn = engine.connect()
    conn.execute(insert, *df.to_dict(orient="record"))


def migrate_data():
    """
    Migrate data *from* S3 into the data warehouse.

    This will delete all existing data in the table before migrating.
    """
    # Clear the table of all existing data
    engine.execute(f'TRUNCATE TABLE "{SCHEMA}"."{TABLE}"')
    # Read the data from s3.
    df = pandas.read_parquet(S3_DATA_PATH)
    check_columns(bike_trips, df)
    # Upload the new data
    insert = sqlalchemy.dialects.postgresql.insert(bike_trips)
    conn = engine.connect()
    conn.execute(insert, *df.to_dict(orient="record"))


def load_to_s3():
    """
    Copy data from the data warehouse *to* S3.
    """
    df = pandas.read_sql_table(TABLE, engine, schema=SCHEMA)
    df.to_parquet(S3_DATA_PATH)


if __name__ == "__main__":
    import sys

    create_table()
    if len(sys.argv) >= 2 and sys.argv[1] == "migrate":
        migrate_data()
    else:
        load_pg_data()
        if not os.environ.get("DEV"):
            load_to_s3()
