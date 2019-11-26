from datetime import datetime, timedelta
from itertools import chain

import pandas as pd
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from geoalchemy2 import Geometry, WKTElement
from shapely.geometry import Point
from sodapy import Socrata

SOCRATA_APP_TOKEN = Variable.get("SOCRATA_APP_TOKEN")
SOCRATA_USERNAME = Variable.get("SOCRATA_USERNAME")
SOCRATA_PASSWORD = Variable.get("SOCRATA_PASSWORD")


def load_to_postgres(**kwargs):
    """
    Loads the care, care plus response codes
    to postgres as an upsert
    """
    dataset_id = "jvre-2ecm"
    client = Socrata(
        "data.lacity.org",
        SOCRATA_APP_TOKEN,
        username=SOCRATA_USERNAME,
        password=SOCRATA_PASSWORD,
    )
    results = []
    req_count = 0
    page_size = 2000
    data = None
    while data != []:
        data = client.get(
            dataset_id,  # view, limited to correct reason codes
            content_type="json",
            offset=req_count * page_size,
            limit=page_size,
        )
        req_count += 1
        results.append(data)
    df = pd.DataFrame.from_dict(list(chain.from_iterable(results)))
    srid = 4326
    df["latitude"] = pd.to_numeric(df["latitude"])
    df["longitude"] = pd.to_numeric(df["longitude"])
    df["geom"] = df.dropna(subset=["latitude", "longitude"]).apply(
        lambda x: WKTElement(Point(x.longitude, x.latitude).wkt, srid=srid), axis=1
    )
    df = df.drop("location", axis=1)

    # Create the connection
    engine = PostgresHook.get_hook("postgres_default").get_sqlalchemy_engine()

    # Write the dataframe to the database
    df.to_sql(
        "311-cases-homelessness",
        engine,
        schema="public-health",
        if_exists="replace",
        dtype={"geom": Geometry("POINT", srid=srid)},
    )

    return True


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 11, 15),
    "email": [
        "hunter.owens@lacity.org",
        "tiffany.chu@lacity.org",
        "ITAData@lacity.org",
    ],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(days=1),
}

dag = DAG("care-plus-loader", default_args=default_args, schedule_interval="@daily")

t1 = PythonOperator(
    task_id="load-to-postgres",
    provide_context=True,
    python_callable=load_to_postgres,
    dag=dag,
)

if __name__ == "__main__":
    load_to_postgres()
