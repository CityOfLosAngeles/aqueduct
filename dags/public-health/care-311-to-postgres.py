from sodapy import Socrata
from itertools import chain
from geoalchemy2 import Geometry, WKTElement
import pandas as pd
from shapely.geometry import Point
from datetime import datetime, timedelta
import os
from sqlalchemy import create_engine
from airflow import DAG
from airflow.hooks.base_hook import BaseHook


def load_to_postgres():
    """
    Loads the care, care plus response codes 
    to postgres as an upsert
    """
    dataset_id = 'jvre-2ecm'
    client = Socrata("data.lacity.org",
                     "PrfihrlGM6GihbDbDRHqtQ0R4",
                     username="hunter.owens@lacity.org",
                     password="e*75BTtTP#Qm*c^N8J7y")

    results = []
    req_count = 0
    page_size = 2000
    data = None
    while data != []: 
        data = client.get(dataset_id, # view, limited to correct reason codes 
                          content_type="json",
                          offset=req_count * page_size,
                          limit=page_size)
        req_count+=1 
        results.append(data) 
    df = pd.DataFrame.from_dict(list(chain.from_iterable(results)))
    srid = 4326
    df['latitude'] = pd.to_numeric(df['latitude'])
    df['longitude'] = pd.to_numeric(df['longitude'])
    df["geom"] = df.dropna(subset=["latitude", "longitude"]).apply(
        lambda x: WKTElement(Point(x.longitude, x.latitude).wkt, srid=srid), axis=1
    )
    df = df.drop('location', axis = 1)
    # Create the connection

    pg_conn = BaseHook.get_connection('postgres_default') 
    user = pg_conn.login
    password = pg_conn.get_password()
    host = pg_conn.host
    dbname = pg_conn.schema
    connection_string = (
        os.environ.get("POSTGRES_URI")
        or f"postgres://{user}:{password}@{host}:5432/{dbname}"
    )

    
    engine = create_engine(connection_string)

        
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
    "email": ["hunter.owens@lacity.org", "tiffany.chu@lacity.org", "ITAData@lacity.org"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(days=1),
}

dag = DAG("care-plus-loader", default_args=default_args, schedule_interval="@daily")

t1 = PythonOperator(
    task_id='load-to-postgres',
    provide_context=True
    python_callable=load_to_postgres,
    dag=dag
)

if __name__ == '__main__': 
    load_to_postgres()