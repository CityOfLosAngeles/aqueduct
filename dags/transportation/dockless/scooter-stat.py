import logging
from datetime import datetime, timedelta

import pandas as pd
import pendulum
import sqlalchemy
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.email import send_email

pg_conn = BaseHook.get_connection("postgres_default")

local_tz = pendulum.timezone("America/Los_Angeles")
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2018, 10, 30, tzinfo=local_tz),
    "email": [
        "hunter.owens@lacity.org",
        "mony.patel@lacity.org",
        "paul.tsan@lacity.org",
    ],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=15)
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(dag_id="scooter-stat", default_args=default_args, schedule_interval="@daily")


def compute_scooter_stats(**kwargs):
    logging.info("Connecting to DB")
    user = pg_conn.login
    password = pg_conn.get_password()
    host = pg_conn.host
    dbname = pg_conn.schema
    logging.info(f"Logging into postgres://-----:----@{host}:5432/{dbname}")
    engine = sqlalchemy.create_engine(
        f"postgres://{user}:{password}@{host}:5432/{dbname}"
    )
    today = kwargs["ds"]
    yesterday = kwargs["yesterday_ds"]
    sum_table_sql = f"""
    SELECT provider_name,
            vehicle_type,
            COUNT(trip_id) as num_trips,
            AVG(trip_distance_miles) as avg_trip_length,
            MAX(trip_distance_miles) as max_trip_length,
            COUNT(trip_id)::FLOAT / COUNT (DISTINCT(device_id)) as avg_rides_per_device,
            COUNT (DISTINCT (device_id)) as num_devices_doing_trips
    FROM v_trips
    WHERE DATE(start_time_local) = '{yesterday}'
    GROUP BY provider_name, vehicle_type ;
    """
    trips = pd.read_sql(
        f"""SELECT * FROM v_trips WHERE end_time_local BETWEEN '{yesterday}' AND '{today}'""",  # noqa: E501
        con=engine,
    )
    status_changes = pd.read_sql(
        f"""SELECT * FROM v_status_changes WHERE event_time_local BETWEEN '{yesterday}' AND '{today}'""",  # noqa: E501
        con=engine,
    )
    sum_table = pd.read_sql(sum_table_sql, con=engine).to_html()
    kwargs["ti"].xcom_push(key="xcom_trips", value=len(trips))
    kwargs["ti"].xcom_push(
        key="xcom_devices", value=len(status_changes.device_id.unique())
    )
    trips_table = pd.DataFrame(
        trips.groupby("provider_name")["trip_id"].count()
    ).to_html()
    device_table = pd.DataFrame(
        status_changes.groupby("provider_name")["device_id"].nunique()
    ).to_html()
    kwargs["ti"].xcom_push(key="trips_table", value=trips_table)
    kwargs["ti"].xcom_push(key="device_table", value=device_table)
    kwargs["ti"].xcom_push(key="sum_table", value=sum_table)
    return True


def email_callback(**kwargs):

    email_template = f"""

    From {kwargs['yesterday_ds']} to {kwargs['ds']}, the number of trips observed was { kwargs['ti'].xcom_pull(key='xcom_trips', task_ids='computing_stats') } across {kwargs['ti'].xcom_pull(key='xcom_devices', task_ids='computing_stats')} devices.

    Company Trips Table:

    { kwargs['ti'].xcom_pull(key='trips_table', task_ids='computing_stats') }

    Company Devices Table:

    { kwargs['ti'].xcom_pull(key='device_table', task_ids='computing_stats') } <br>

    Status Table <br>
    { kwargs['ti'].xcom_pull(key='sum_table', task_ids='computing_stats')}

    """  # noqa: E501
    send_email(
        to=[
            "hunter.owens@lacity.org",
            "marcel.porras@lacity.org",
            "jose.elias@lacity.org",
            "paul.tsan@lacity.org",
            "vladimir.gallegos@lacity.org",
            "sean@ellis-and-associates.com",
            "john@ellis-and-associates.com",
            "max@ellis-and-associates.com",
            "ian.rose@lacity.org",
        ],
        subject=f"Dockless Stats for { kwargs['yesterday_ds'] }",
        html_content=email_template,
    )

    return True


send_stats_email = PythonOperator(
    task_id="scoot_stat_email",
    python_callable=email_callback,
    provide_context=True,
    dag=dag,
)
compute_stats = PythonOperator(
    task_id="computing_stats",
    provide_context=True,
    python_callable=compute_scooter_stats,
    dag=dag,
)

compute_stats >> send_stats_email
