from datetime import datetime, timedelta

import arcgis

from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 1, 30),
    "email": [
        "ian.rose@lacity.org",
        "hunter.owens@lacity.org",
        "brendan.bailey@lacity.org",
    ],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(days=1),
}

dag = DAG("code-75", default_args=default_args, schedule_interval="@daily")


def update_code_75(**kwargs):
    # Connecting to ArcGIS
    arcconnection = BaseHook.get_connection("arcgis")
    gis = arcgis.GIS(
        "http://lahub.maps.arcgis.com/", arcconnection.login, arcconnection.password
    )

    # Getting Code 75 data
    gis_item = gis.content.get(kwargs.get("arcfeatureid"))
    sdf = gis_item.layers[0].query().sdf
    sdf.drop("SHAPE", axis=1, inplace=True)

    # Creating table if does not exist
    engine = PostgresHook.get_hook("postgres_default").get_sqlalchemy_engine()
    engine.connect().execute('CREATE SCHEMA IF NOT EXISTS "public-health"')
    create_table_statement = """CREATE TABLE IF NOT EXISTS "public-health"."code75s" (
    index BIGINT,
    "OBJECTID" BIGINT,
    address TEXT,
    cd FLOAT(53),
    closeddate TEXT,
    createddate TEXT,
    latitude FLOAT(53),
    longitude FLOAT(53),
    reasoncode BIGINT,
    resolutioncode TEXT,
    srnumber TEXT,
    status TEXT
    )"""
    engine.connect().execute(create_table_statement)

    # Deleting old records
    object_id_list = ",".join(list(sdf["OBJECTID"]))
    engine.connect().execute(
        'DELETE FROM "public-health".code75s WHERE "OBJECTID" IN (%s)' % object_id_list
    )

    # Sending updates and new records to postgres
    sdf.to_sql(
        "code75s", engine, schema="public-health", if_exists="append",
    )


t1 = PythonOperator(
    task_id="Update_Code_75_Data",
    provide_context=True,
    python_callable=update_code_75,
    op_kwargs={"arcfeatureid": "6b757be10dd04edb9c139894805425a1",},
    dag=dag,
)
