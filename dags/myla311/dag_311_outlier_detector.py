#!/usr/bin/env python
# coding: utf-8

# Prerequisite:
# 1. the database contains the whole week's data of last week until last Sunday.
#    e.g. if today is 9/26/18 Wed, it must contains the data until 9/23/18 Sunday
#
# The program uses ISO Calendar:
# 1. first day and last day of the week are respectively Monday(1) and Sunday(7)
# 2. the last few days could be counted as in the first week of the next year
#    e.g. 2014-12-31 is in the week01 of 2015
#    vice versa:
#    e.g. 2016-01-01 is in the week53 of 2015
#
# If Gmail is used to receive Outlier Alert emails,
#   the gmail account's 2 step verification has to be disabled,
#   and access from less secure apps should be allowed.
#
# Environment Variables:
#   the following variables need to be setup as environment variables
#     CONNECTION_ID = "postgres_default" # postgres connection id
#     smtp_host = 'smtp.gmail.com'
#     smtp_starttls = True
#     smtp_ssl = True
#     smtp_user = 'email address owner's email account'
#     smtp_password = 'email account password'
#     smtp_port = '587'
#     smtp_mail_from = 'email address to send from'
#     email_to = ['address1', 'address2',..] # can add more receipients
#     email_cc = ['address3', 'address4',..]

import os

import logging
import airflow
import smtplib
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from config import *

# a csv file with this filename will be saved from a copy of postgres table
filename = "./myla311-postgres.csv"

# email parameters
message = "This last week's Outliers:\n\n"
email_to = ', '.join(email_to) # should be setup as environment variable, e.g. email_to = ['address1', 'address2']
email_cc = ', '.join(email_cc) # same as above
subject = '[Alert] MyLA311 Data Outliers'

# outlier type identifiers
INDIV_HIGH_OUTLIER = 'HIGH INDIVIDUAL REQUEST TYPE OUTLIER'
INDIV_LOW_OUTLIER = 'LOW INDIVIDUAL REQUEST TYPE OUTLIER'
TOTAL_HIGH_OUTLIER = 'HIGH TOTAL OUTLER'
TOTAL_LOW_OUTLIER = 'LOW TOTAL OUTLIER'
DIFF_HIGH_OUTLIER = 'HIGH TOTAL DIFF OUTLIER'
DIFF_LOW_OUTLIER = 'LOW TOTAL DIFF OUTLIER'
HIGH_PROCESS_TIME_OUTLIER = 'HIGH PROCESS TIME OUTLIER'
LOW_PROCESS_TIME_OUTLIER = 'LOW PROCESS TIME OUTLIER'

def detect_outliers(**kwargs):

    """
    Outlier Detector that detects the following types of outliers:

        1. number of Individual Request Type per week, high and low outliers
        2. number of Total requests per week, high and low outliers
        3. the difference between last week and the week before
        4. request process time high and low outliers
    """

    # Retrieve data
    df = pd.read_csv(filename, index_col=False)

    # change data type from object to datatime
    df['createddate'] = pd.to_datetime(df['createddate'], errors='coerce')
    df['closeddate'] = pd.to_datetime(df['closeddate'], errors='coerce')

    df.sort_values(by='createddate', inplace=True)
    df.rename(
        columns={'createddate': 'created_datetime', 'closeddate': 'closed_datetime', 'requesttype': 'request_type'},
        inplace=True)

    df['created_date'] = df['created_datetime'].dt.date

    # set index to created_date
    df.set_index('created_date', drop=False, inplace=True)

    # create week number and year cols based on ISO Calendar
    df['cal_week_number'] = df['created_datetime'].apply(lambda x: x.isocalendar()[1])
    df['cal_year'] = df['created_datetime'].apply(lambda x: x.isocalendar()[0])

    # padding the week number to two digit number, and
    # combine with the 'created_year' into a created_year.week_number column called 'year_week'
    df['year_week'] = df['cal_year'].map(str) + "." + df['cal_week_number'].apply(
        lambda x: '0' + str(x) if x < 10 else x).map(str)

    # Preparing Data for Request Type Outlier Detector
    pivot_yw = df.groupby(['year_week', 'request_type']).size().unstack()
    pivot_yw['Total'] = pivot_yw.sum(axis=1)

    last_week = (datetime.today() - timedelta(7)).isocalendar()
    last_week_num = last_week[1]
    last_week_year = last_week[0]
    last_yw = '.'.join([str(last_week_year), str(last_week_num)])

    same_week_of_last_year = (datetime.today() - timedelta(days=365)).isocalendar()
    week_num_swoly = same_week_of_last_year[1]
    year_swoly = same_week_of_last_year[0]
    yw_a_year_ago = '.'.join([str(year_swoly), str(week_num_swoly)])

    df_last_whole_yr = pivot_yw.loc[yw_a_year_ago:last_yw]
    df_last_week = df_last_whole_yr.loc[last_yw]

    cols = df_last_whole_yr.columns

    # make the alert dict
    alert = {}

    # Get individual requet_type outliers
    for x in cols:
        if df_last_week[x] > df_last_whole_yr[x].mean() + 2 * df_last_whole_yr[x].std():
            alert[x] = INDIV_HIGH_OUTLIER
        elif df_last_week[x] < df_last_whole_yr[x].mean() - 2 * df_last_whole_yr[x].std():
            alert[x] = INDIV_LOW_OUTLIER

    # Get weekly total outliers
    high_bound = df_last_whole_yr['Total'].mean() + 2 * df_last_whole_yr['Total'].std()
    low_bound = df_last_whole_yr['Total'].mean() - 2 * df_last_whole_yr['Total'].std()
    if df_last_week['Total'] > high_bound:
        alert['Total High'] = TOTAL_HIGH_OUTLIER
    elif df_last_week['Total'] < low_bound:
        alert['Total Low'] = TOTAL_LOW_OUTLIER

    # Get Diff of weekly total outliers
    pd.options.mode.chained_assignment = None  # turn off Value to be set on a copy of a slice warning
    df_last_whole_yr['Diff'] = df_last_whole_yr['Total'].diff()
    df_last_whole_yr['Diff'] = df_last_whole_yr['Diff'].abs()
    pd.options.mode.chained_assignment = "warn"

    high_bound = df_last_whole_yr['Diff'].mean() + 2 * df_last_whole_yr['Diff'].std()
    low_bound = df_last_whole_yr['Diff'].mean() - 2 * df_last_whole_yr['Diff'].std()
    if df_last_whole_yr['Diff'][last_yw] > high_bound:
        alert['Diff High'] = DIFF_HIGH_OUTLIER
    elif df_last_whole_yr['Diff'][last_yw] < low_bound:
        alert['Diff Low'] = DIFF_LOW_OUTLIER

    # Get process time outliers
    df['process_time'] = (df['closed_datetime'] - df['created_datetime']).apply(lambda x: x.total_seconds())
    df_process_time = df[['srnumber', 'created_date', 'request_type', 'process_time']]

    df_process_time.dropna(inplace=True)

    # get the first day of the week of the same day of the last year\
    same_day_of_last_year = (datetime.today() - relativedelta(years=1)).date()
    delta_to_first_day_of_that_week = same_day_of_last_year.isocalendar()[2] - 1
    first_day_of_last_yr_week = same_day_of_last_year - timedelta(days=delta_to_first_day_of_that_week)

    # Get the last day of last week
    one_week_ago = (datetime.today() - timedelta(days=7)).date()
    last_week_start = one_week_ago - timedelta(days=one_week_ago.weekday())
    last_week_end = last_week_start + timedelta(days=6)

    df_process_time = df_process_time[first_day_of_last_yr_week : last_week_end]
    df_proc_time_last_week = df_process_time.loc[last_week_start : last_week_end]

    mean = df_process_time['process_time'].mean()
    std = df_process_time['process_time'].std()
    high_bound = mean + 2 * std
    low_bound = mean - 2 * std
    for index, row in df_proc_time_last_week.iterrows():
        if row['process_time'] > high_bound:
            alert['srnumber: ' + row['srnumber']] = HIGH_PROCESS_TIME_OUTLIER + ", " + row['request_type'] + ", " \
                                                    + str(int(round(row['process_time'] / 3600, 1))) + " hrs"
        elif row['process_time'] < low_bound:
            alert['srnumber: ' + row['srnumber']] = LOW_PROCESS_TIME_OUTLIER + ", " + row['request_type'] + ", " \
                                                    + str(int(round(row['process_time'] / 3600, 1))) + " hrs"
    logging.info('Alert: ' + str(alert))
    return alert

def sendemail(from_addr, to_addr_list, cc_addr_list, subject,
              message, login, password, smtpserver, **kwargs):
    # xcom_pull alert from last task's return value
    task_instance = kwargs['task_instance']
    alert = task_instance.xcom_pull(task_ids='detect_outliers')
    # if no outliers found, skip sending email
    if bool(alert):
        # prepare email body
        logging.info(alert)
        count = 1
        for key, value in alert.items():
            # print(key, value)
            message += str(count) + ". " + key + ": " + value + "\n"
            # print(message)
            count += 1

        msg = MIMEMultipart()
        msg['From'], msg['To'], msg['Cc'], msg['Subject'] = from_addr, to_addr_list, cc_addr_list, subject # specify your sender, receiver, subject attributes

        body = MIMEText(message, 'plain')  # convert the body to a MIME compatible string
        msg.attach(body)

        server = smtplib.SMTP(smtpserver)
        server.starttls()
        server.login(login, password)
        server.sendmail(msg['From'], [to_addr_list, cc_addr_list], msg.as_string())
        return "Outliers founded. Alert Email sent. Success!"

    return "No alert generated. Email not sent."


sql_pull_data = \
    """
    COPY myla311_main TO '{}' DELIMITER ',' CSV HEADER;
    """.format(os.path.abspath(filename))

# airflow DAG arguments
args = {'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(7),
    'provide_context': True,
    # 'retries': 2, # will be set after testing
    # 'retry_delay': timedelta(minutes=5)
    }

# initiating the DAG
dag = airflow.DAG(
    dag_id='outlier_detector',
    schedule_interval="@weekly",
    default_args=args,
    max_active_runs=1)

task0 = PostgresOperator(
    task_id='pull_data_from_postgres',
    sql=sql_pull_data.format(filename),
    postgres_conn_id=CONNECTION_ID,
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
    op_args=[smtp_mail_from, email_to, email_cc, subject, message,
             smtp_user, smtp_password, smtp_host + ':' + smtp_port],
    python_callable=sendemail,
    dag=dag)

task0 >> task1 >> task2
