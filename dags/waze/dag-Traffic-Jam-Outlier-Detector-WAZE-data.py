import os
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from mapboxgl.viz import LinestringViz
from mapboxgl.utils import create_color_stops, create_numeric_stops
from geojson import Feature, FeatureCollection, LineString

import logging
import airflow
from datetime import datetime, timedelta
#from dateutil.relativedelta import relativedelta
from pytz import timezone
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.email import send_email
from airflow.models import Variable

#categoried by road_types. from 10mil rows of data, outlier upperbounds -- mean+2std:
upperbound_delay_rt1_2std = 364.2580985017166 
upperbound_delay_rt2_2std = 416.00629003480043
upperbound_delay_rt3_2std = 1337.7260891639444
upperbound_delay_rt4_2std = 393.1583664937325
upperbound_delay_rt6_2std = 494.9713645058797
upperbound_delay_rt7_2std = 366.12815975980857
upperbound_delay_rt17_2std = -1.0
upperbound_delay_rt20_2std = -1.0

message = f"This Today's Traffic Jam Outliers during 5pm-6pm by Road Type categories:"
email_to = "hunter.owens@lacity.org" 
email_cc = "lei.cao@lacity.org"
subject = "[Alert] Today's Traffic Jam Outliers"

# Mapbox token
token = os.getenv('MAPBOX_ACCESS_TOKEN')

# Postgres Database login info
USER = os.getenv('WAZE_USER')
PASS = os.getenv('WAZE_PASS')

# Remote Database Info
HOST = 'datalake.cyuk6whwgqww.us-west-2.rds.amazonaws.com'
PORT = '5432'
DB = 'db_datalake'

uri = f'postgresql://{USER}:{PASS}@{HOST}:{PORT}/{DB}'
engine = create_engine(uri)


def retrieve_data(**kwarg):
    """
    For now, just read the 10mil-row csv. 
    When we have new data from WAZE, this part will be the data in the time window only
    """
    logging.info("start loading jams dataset from remote")
    # df = pd.read_sql_query("select * from waze.jams limit 10000000",con=engine)
    # df.to_csv("/tmp/waze.jams_10mil.csv")
    logging.info("jams dataset is saved to /tmp/waze.jams_10mil.cs")

    logging.info("start loading data_files dataset from remote")
    df_datafiles = pd.read_sql_query("select * from waze.data_files;", con=engine)
    df_datafiles.to_csv("/tmp/waze.datafiles.csv", index=False)
    logging.info("data_files dataset is saved to /tmp/waze.datafiles.csv")

    # clean and leave only what we need
    df_datafiles = df_datafiles[['id', 'start_time', 'end_time']]


def detect_outliers(**kwarg):
    logging.info('Loading the datasets...')
    df = pd.read_csv('/tmp/waze.jams_10mil.csv')
    df_datafiles = pd.read_csv('/tmp/waze.datafiles.csv')
    logging.info('Loading completed! Starting Detection...')

    # Check LA city ONLY
    df = df[df.city == 'Los Angeles, CA']

    # add time info to jams data
    df_jam_w_time = pd.merge(df[['id', 'datafile_id', 'street', 'road_type', 'delay', 'length', 'line']], df_datafiles, left_on='datafile_id', right_on='id')
    df_jam_w_time.drop(columns=['id_y'], inplace=True)
    df_jam_w_time.rename({'id_x':'jam_id'}, axis='columns', inplace=True)

    # We don't actually need 'end_time' column. In production, we can drop this column
    df_jam_w_time['start_time'] = pd.to_datetime(df_jam_w_time['start_time'])
    # df_jam_w_time['end_time'] = pd.to_datetime(df_jam_w_time['end_time'])

    # Add time zone info
    df_jam_w_time['start_time'] = df_jam_w_time['start_time'].apply(timezone('UTC').localize)
    # df_jam_w_time['end_time'] = df_jam_w_time['end_time'].apply(timezone('UTC').localize)
    df_jam_w_time.rename({'start_time':'start_time_utc', 'end_time':'end_time_utc'}, axis='columns', inplace=True)

    logging.info('In Progress...')

    # Adding PST timezone
    df_jam_w_time['start_time_pst'] = df_jam_w_time['start_time_utc'].apply(lambda x: x.astimezone(timezone('US/Pacific')))
    # df_jam_w_time['end_time_pst'] = df_jam_w_time['end_time_utc'].apply(lambda x: x.astimezone(timezone('US/Pacific')))

    logging.info('In Progress...')

    # Delay Outliers in a time window
    # Check a time window on a Friday afternoon

    df_jam_time_index = df_jam_w_time.set_index('start_time_pst', drop=False)
    df_jam_time_index.sort_index(inplace=True)

    # set the time window
    time_window_start = datetime(2017, 12, 15, 17,0,0, tzinfo=timezone('US/Pacific'))
    time_window_end = datetime(2017, 12, 15, 18,0,0, tzinfo=timezone('US/Pacific'))

    df_time_window = df_jam_time_index.loc[time_window_start:time_window_end]

    if len(df_time_window) == 0:
        logging.info('No data available in the given Time Window')
        return

    # Get outliers in categories of road types
    """
    Road Types - provided by WAZE data description

    Value Type
    1 Streets
    2 Primary Street
    3 Freeways
    4 Ramps
    5 Trails            -- NA in the dataset
    6 Primary
    7 Secondary
    8,14 4X4 Trails     -- NA in the dataset
    15 Ferry crossing   -- NA in the dataset
    9 Walkway           -- NA in the dataset
    10 Pedestrian       -- NA in the dataset
    11 Exit             -- NA in the dataset
    16 Stairway         -- NA in the dataset
    17 Private road
    18 Railroads        -- NA in the dataset
    19 Runway/Taxiway   -- NA in the dataset
    20 Parking lot road
    21 Service road     -- NA in the dataset
    """

    df_roadtype_1 = df_jam_w_time[df_jam_w_time.road_type == 1]
    df_roadtype_2 = df_jam_w_time[df_jam_w_time.road_type == 2]
    df_roadtype_3 = df_jam_w_time[df_jam_w_time.road_type == 3]
    df_roadtype_4 = df_jam_w_time[df_jam_w_time.road_type == 4]
    df_roadtype_6 = df_jam_w_time[df_jam_w_time.road_type == 6]
    df_roadtype_7 = df_jam_w_time[df_jam_w_time.road_type == 7]
    df_roadtype_17 = df_jam_w_time[df_jam_w_time.road_type == 17]
    df_roadtype_20 = df_jam_w_time[df_jam_w_time.road_type == 20]

    # The Outlier Upperbounds are calculated and stored as variables at the top

    # upperbound_delay_rt1_2std = df_roadtype_1.delay.mean() + 2 * df_roadtype_1.delay.std()
    # upperbound_delay_rt2_2std = df_roadtype_2.delay.mean() + 2 * df_roadtype_2.delay.std()
    # upperbound_delay_rt3_2std = df_roadtype_3.delay.mean() + 2 * df_roadtype_3.delay.std()
    # upperbound_delay_rt4_2std = df_roadtype_4.delay.mean() + 2 * df_roadtype_4.delay.std()
    # upperbound_delay_rt6_2std = df_roadtype_6.delay.mean() + 2 * df_roadtype_6.delay.std()
    # upperbound_delay_rt7_2std = df_roadtype_7.delay.mean() + 2 * df_roadtype_7.delay.std()
    # upperbound_delay_rt17_2std = df_roadtype_17.delay.mean() + 2 * df_roadtype_17.delay.std()
    # upperbound_delay_rt20_2std = df_roadtype_20.delay.mean() + 2 * df_roadtype_20.delay.std()

    #
    delay_outliers_rt1 = df_time_window[(df_time_window.road_type == 1) &                                     
                                        ((df_time_window.delay > upperbound_delay_rt1_2std) |                                     
                                        (df_time_window.delay == -1))]
    delay_outliers_rt1.name = "1-Street"

    delay_outliers_rt2 = df_time_window[(df_time_window.road_type == 2) &                                    
                                        ((df_time_window.delay > upperbound_delay_rt2_2std) |                                      
                                        (df_time_window.delay == -1))]
    delay_outliers_rt2.name = "2-Primary Street"

    delay_outliers_rt3 = df_time_window[(df_time_window.road_type == 3) &                                     
                                        ((df_time_window.delay > upperbound_delay_rt3_2std) |                                      
                                        (df_time_window.delay == -1))]
    delay_outliers_rt3.name = "3-Freeways"

    delay_outliers_rt4 = df_time_window[(df_time_window.road_type == 4) &                                     
                                        ((df_time_window.delay > upperbound_delay_rt4_2std) |                                      
                                        (df_time_window.delay == -1))]
    delay_outliers_rt4.name = "4-Ramps"

    delay_outliers_rt6 = df_time_window[(df_time_window.road_type == 6) &                                     
                                        ((df_time_window.delay > upperbound_delay_rt6_2std) |                                      
                                        (df_time_window.delay == -1))]
    delay_outliers_rt6.name = "6-Primary"

    delay_outliers_rt7 = df_time_window[(df_time_window.road_type == 7) &                                     
                                        ((df_time_window.delay > upperbound_delay_rt7_2std) |                                      
                                        (df_time_window.delay == -1))]
    delay_outliers_rt7.name = "7-Secondary"

    delay_outliers_rt17 = df_time_window[(df_time_window.road_type == 17) &                                     
                                         ((df_time_window.delay > upperbound_delay_rt17_2std) |                                      
                                         (df_time_window.delay == -1))]
    delay_outliers_rt17.name = "17-Private Road"

    delay_outliers_rt20 = df_time_window[(df_time_window.road_type == 20) &                                     
                                         ((df_time_window.delay > upperbound_delay_rt20_2std) |                                      
                                         (df_time_window.delay == -1))]
    delay_outliers_rt20.name = "20-Parking lot road"

    ls_delay_outliers = [delay_outliers_rt1, delay_outliers_rt2, delay_outliers_rt3, delay_outliers_rt4, 
                        delay_outliers_rt6, delay_outliers_rt7, delay_outliers_rt17, delay_outliers_rt20]

    # make a helper function that will make the input for graph making tool
    def getGeoJson(lines):
        count=0
        feature_coll = []
        p = {'sample': 450, 'weight': 1.5}
        for line in lines:
            points = []
            line = eval(line)
            for point in line:
                points.append((point['x'], point['y']))
            feature_coll.append(Feature(geometry=LineString(points), properties=p))
        return FeatureCollection(feature_coll)

    # function that make map and save as html
    def show_in_map(df):
        
        logging.info(df.name)
        viz = LinestringViz(getGeoJson(df.line), 
                            access_token=token,
                            color_property='sample',
                            color_stops=create_color_stops([0, 350, 400, 500, 600], colors='Reds'),
                            line_stroke='--',
                            line_width_property='weight',
                            line_width_stops=create_numeric_stops([0, 1, 2, 3, 4, 5], 0, 10),
                            opacity=0.8,
                            center = (-118.2427, 34.0537),
                            zoom = 9,
                            below_layer='waterway-label')
        # viz.show()
        html = open(f"/tmp/{df.name}-outliers.html", "w")
        html.write(viz.create_html())
        html.close()
        return f"/tmp/{df.name}-outliers.html"

    logging.info('Making Maps ...')

    # a dict to hold the outlier street names
    outliers = {}

    # make and save map
    for x in ls_delay_outliers:
        if len(x) != 0:
            logging.info(f"Outliers found for {x.name}")
            outliers[x.name] = [show_in_map(x)]
            outliers[x.name].append(x.street.unique())
        else:
            logging.info(f"No outliers found for {x.name}")
    

    return outliers


def sendemail(to_addr_list, cc_addr_list, subject,
              message, **kwargs):
    # xcom_pull alert from last task's return value
    task_instance = kwargs['task_instance']
    outliers = task_instance.xcom_pull(task_ids='detect_outliers')
              
    # if no outliers found, skip sending email
    if bool(outliers):
        # prepare email body

        files = []
        for key, value in outliers.items():
            files.append(value[0])

        html_content = make_html_content(outliers, message)
        send_email(to_addr_list, subject, html_content,
               files=files,  cc=cc_addr_list)
        return "Outliers founded. Alert Email sent. Success!"

    return "No alert generated. Email not sent."


def make_html_content(outliers, message):
    count = 1
    c = message.join(['<br><b>', '</b></br><br />'])
    for key, value in outliers.items():
        c += f"{key}:{value[1]}<br/><br/>"
        count += 1
    return c


def remove_map_html(**kwargs):
    task_instance = kwargs['task_instance']
    outliers = task_instance.xcom_pull(task_ids='detect_outliers')

    for key, value in outliers.items():
        file = value[0]
        if os.path.exists(file):
          os.remove(file)
        else:
          print("The file "+ file + " does not exist. Skipping to the next map html")


# airflow DAG arguments
args = {'owner': 'hunterowens',
    'start_date': airflow.utils.dates.days_ago(7),
    'provide_context': True,
    'email': 'hunter.owens@lacity.org',
    'email_on_failure': False,
    'retries': 1, 
    'retry_delay': timedelta(minutes=5)
    }

# initiating the DAG
dag = airflow.DAG(
    dag_id='traffic-jam-outlier_detector',
    schedule_interval="@daily",
    default_args=args,
    max_active_runs=1)

task0 = PythonOperator(
    task_id='retrieve_data',
    provide_context=True,
    python_callable=retrieve_data,
    dag=dag)

task1 = PythonOperator(
    task_id='detect_outliers',
    provide_context=True,
    python_callable=detect_outliers,
    dag=dag)

task2 = PythonOperator(
    task_id='send_email_if_outliers',
    provide_context=True,
    # all the variable used below should be setup as environment variable
    op_args=[email_to, email_cc, subject, message],
    python_callable=sendemail,
    dag=dag)

task3 = PythonOperator(
    task_id='remove_map_html',
    provide_context=True,
    python_callable=remove_map_html,
    dag=dag)

task0 >> task1 >> task2 >> task3