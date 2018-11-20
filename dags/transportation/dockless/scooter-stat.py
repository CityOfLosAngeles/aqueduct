import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.email_operator import EmailOperator
from airflow.hooks.base_hook import BaseHook
from datetime import datetime, timedelta
import logging
import sqlalchemy
import pandas as pd

pg_conn = BaseHook.get_connection('postgres_default') 


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 10, 30), 
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
    dag_id = 'scooter-stat',
    default_args=default_args,
    schedule_interval='@daily'
    )

refresh_materialized_status = """
REFRESH MATERIALIZED VIEW v_status_changes;
"""

refresh_materialized_trips = """
REFRESH MATERIALIZED VIEW v_trips; 
""" 

task1 = PostgresOperator(
    task_id='update_materilized_view_status',
    sql=refresh_materialized_status,
    postgres_conn_id='postgres_default',
    dag=dag
    )


task2 = PostgresOperator(
    task_id='update_materilized_view_trips',
    sql=refresh_materialized_trips,
    postgres_conn_id='postgres_default',
    dag=dag
    )

def set_xcom_variables(**kwargs):
    logging.info("Connecting to DB")
    user = pg_conn.login
    password = pg_conn.get_password()
    host = pg_conn.host
    dbname = pg_conn.schema
    logging.info(f"Logging into postgres://-----:----@{host}:5432/{dbname}")
    engine = sqlalchemy.create_engine(f'postgres://{user}:{password}@{host}:5432/{dbname}')
    today = kwargs['ds']
    yesterday = kwargs['yesterday_ds']
    trips = pd.read_sql(f"""SELECT * FROM TRIPS WHERE end_time BETWEEN '{yesterday}' AND '{today}'""", 
                        con=engine)
    kwargs['ti'].xcom_push(key='xcom_trips', value = len(trips))
    kwargs['ti'].xcom_push(key='xcom_devices', value = len(trips.device_id.unique()))
    trips_table = pd.DataFrame(trips.groupby('provider_name')['trip_id'].count()).to_html()
    device_table = pd.DataFrame(trips.groupby('provider_name')['device_id'].nunique()).to_html()
    kwargs['ti'].xcom_push(key='trips_table', value=trips_table)
    kwargs['ti'].xcom_push(key='device_table', value=device_table)
    return True

email_template = """

In the last 24 hours, the number of trips observed was {{ task_instance.xcom_pull(key='xcom_trips', task_ids='computing_stats') }} across {{ task_instance.xcom_pull(key='xcom_devices', task_ids='computing_stats') }} devices. 

"""

spare_text = """
Company Trips Table: 

{{ task_instance.xcom_pull(key='trips_table', task_ids='computing_stats') }}

Company Devices Table: 

{{ task_instance.xcom_pull(key='device_table', task_ids='computing_stats') }}

"""

alert_email = EmailOperator(
    task_id="scoot_stat_email",
    to=['hunter.owens@lacity.org', 'marcel.porras@lacity.org', 'jose.elias@lacity.org', 'timothy.black@lacity.org'],
    subject='Scooter Stat {{ts.date}}',
    html_content=email_template,
    dag=dag
)

set_xcom = PythonOperator(
        task_id = "computing_stats",
        provide_context=True,
        python_callable=set_xcom_variables,
        dag=dag)

task1.set_downstream(set_xcom)
task2.set_downstream(set_xcom)
alert_email.set_upstream(set_xcom)
