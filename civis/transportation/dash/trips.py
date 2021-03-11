"""
Download LADOT Downtown DASH data, and upload it to Postgres and S3.
"""
import os
from base64 import b64encode
from urllib.parse import quote_plus

import pandas
import requests
import sqlalchemy

S3_BUCKET = "s3://tmf-ita-data/dash"
SCHEMA = "transportation"
TABLE = "dash_trips"
LOCAL_TIMEZONE = "US/Pacific"


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

# Get data from the previous day
yesterday = (pandas.Timestamp.now() - pandas.Timedelta(days=1)).date()

# Define the PostgreSQL Table using SQLAlchemy
metadata = sqlalchemy.MetaData(schema=SCHEMA)
dash_trips = sqlalchemy.Table(
    TABLE,
    metadata,
    # Add the columns
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
    sqlalchemy.Column("trip_id", sqlalchemy.Integer),
    sqlalchemy.Column("trip_href", sqlalchemy.String),
    sqlalchemy.Column("trip_name", sqlalchemy.String),
    sqlalchemy.Column("vehicle_href", sqlalchemy.String),
    sqlalchemy.Column("vehicle_name", sqlalchemy.String),
    # Add constraints. We want to consider a single stop on a single day
    # to be unique, so we combine 'trip_id' (which is shared across multiple
    # days and stops) with 'stop_name' and 'scheduled_arrive'
    sqlalchemy.UniqueConstraint("scheduled_arrive", "stop_name", "trip_id"),
)


def get_bearer_token():
    user = os.environ.get("SYNCROMATICS_USERNAME")
    password = os.environ.get("SYNCROMATICS_PASSWORD")
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
        print(
            f"Checking that {column.name}'s type {column.type} "
            f"is consistent with {df.dtypes[column.name]}"
        )
        assert type_map[str(column.type)] == str(df.dtypes[column.name])


def create_table():
    """
    Create the schema/tables to hold the hare data.
    """
    print("Creating tables")
    if not engine.dialect.has_schema(engine, SCHEMA):
        engine.execute(sqlalchemy.schema.CreateSchema(SCHEMA))
    metadata.create_all(engine)


def load_pg_data():
    """
    Query trips data from the Syncromatics REST API and upload it to Postgres.
    """
    # Fetch the data from the rest API for the previous day.
    DOWNTOWN_DASH_ID = "LADOTDT"
    token = get_bearer_token()
    print(f"Fetching DASH data for {yesterday}")
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
    # The trips may be zero due to holidays or missing data.
    if len(df) == 0:
        print("No trips found -- is this a holiday?")
        return

    # Drop unnecesary driver info.
    df = df.drop(columns=["driver_first_name", "driver_last_name"])

    # Drop null trip ids and make sure they are integers.
    df = df.dropna(subset=["trip_id"])
    df.trip_id = df.trip_id.astype("int64")

    # Set the timezone to local time with TZ info
    for col in time_cols:
        df[col] = df[col].dt.tz_convert(LOCAL_TIMEZONE)

    check_columns(dash_trips, df)

    # Upload the final dataframe to Postgres. Since pandas timestamps conform to the
    # datetime interface, psycopg can correctly handle the timestamps upon insert.
    print("Uploading to PG")
    insert = sqlalchemy.dialects.postgresql.insert(dash_trips).on_conflict_do_nothing()
    conn = engine.connect()
    conn.execute(insert, *df.to_dict(orient="record"))


def load_to_s3(date):
    """
    Load the table from PG and upload it as a parquet to S3.
    """
    print("Uploading data to s3")
    sql = f"""
    SELECT *
    FROM "{SCHEMA}"."{TABLE}"
    WHERE DATE(scheduled_depart AT TIME ZONE 'PST') = '{date}'
    """
    df = pandas.read_sql_query(sql, engine)
    if len(df) == 0:
        print(f"Got no trips for {date}. Exiting early")
        return

    path = f"{S3_BUCKET}/dash-trips-{date}.parquet"
    # Write to parquet, allowing timestamps to be truncated to millisecond.
    # This is much more precision than we will ever need or get.
    df.to_parquet(path, allow_truncated_timestamps=True)


def migrate_data():
    """
    Migrate data *from* S3 into the data warehouse.

    This will delete all existing data in the table before migrating.
    """
    # Clear the table of all existing data
    engine.execute(f'TRUNCATE TABLE "{SCHEMA}"."{TABLE}"')
    # Read the data from s3.
    df = pandas.read_parquet("s3://tmf-data/dash-trips.parquet", engine="pyarrow")
    # Upload the new data
    insert = sqlalchemy.dialects.postgresql.insert(dash_trips).on_conflict_do_nothing()
    conn = engine.connect()
    conn.execute(insert, *df.to_dict(orient="record"))


if __name__ == "__main__":
    import sys

    create_table()
    if len(sys.argv) >= 2 and sys.argv[1] == "migrate":
        migrate_data()
    else:
        load_pg_data()
        if not os.environ.get("DEV"):
            load_to_s3(yesterday)
