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
#   the following variables need to be setup in airflow.cfg
#     smtp_host = 'smtp.gmail.com'
#     smtp_starttls = True
#     smtp_ssl = True
#     smtp_user = 'email address owner's email account'
#     smtp_password = 'email account password'
#     smtp_port = '587'
#     smtp_mail_from = 'email address to send from'
#   the folllowing variables need to be setup airflow's webserver UI: Admin -> Variables
#     email_to = 'address1, address2' 
#     email_cc = 'address3, address4'


"""

This DAG is to perform Outlier Detection for each individual Council District of LA city

"""

import os
import logging
import airflow
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.email import send_email
from airflow.models import Variable
import matplotlib.pyplot as plt
from matplotlib import *
import seaborn as sns
import altair as alt

# a csv file with this filename will be saved from a copy of postgres table
filename = "/tmp/myla311.csv"
prefix = "/tmp/"

# get the current Council District data and init cd_dict
cd_list = pd.read_csv("./LA_City_Council_Districts.csv", index_col=False)
cd_list.OBJECTID = cd_list.OBJECTID.astype(float)
cd_dict={}
for index, row in cd_list.iterrows():
    cd_dict[row.OBJECTID] = row.NAME
no_cd = len(cd_dict)

# email parameters Notice: email addresses need to updated in production
message = "This last week's Outliers for Council District {} of LA City:"
test = "hunter.owens@lacity.org,donna.arrechea@lacity.org" 
test1 = "lei.cao@lacity.org"
email_to = {key: test for key in cd_dict.keys()}
email_cc = {key: test1 for key in cd_dict.keys()}
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


def make_save_graph(df, cd, col, title):
    line = alt.Chart(df.reset_index(), title = ' - '.join([title, col])).mark_line().encode(
    x='year_week:O',
    y= col+':Q')
    rule = alt.Chart(df).mark_rule(color='red').encode(
    alt.Y('average({})'.format(col)),
    size=alt.value(1))
    graph = line + rule
    filename = 'chart-cd{}-{}.png'.format(int(cd), col.replace('/', '-').replace(' ','-'))
    graph.save(prefix + filename)
    return filename


def make_save_boxplot(df, cd, point,title):
    # using seaborn
    sns.set_style("whitegrid")
    fig, ax = plt.subplots(figsize=(8,8))
    ax.set_title(title, fontsize=15)
    plot1 = sns.boxplot(ax=ax, x=df, linewidth=1, color='lightblue')
    plot2 = plt.scatter(point, 0, marker='o', s=100, c='red', linewidths=5,label='Outlier')
    ax.legend()
    filename = 'chart-cd{}-Proc-Time-{}.png'.format(int(cd), title.replace('/', '-').replace(' ','-'))
    plt.savefig(prefix + filename)
    plt.close()
    return filename


def detect_outliers(filename, **kwargs):

    """
    Outlier Detector that detects the following types of outliers:

        1. number of Individual Request Type per week, high and low outliers
        2. number of Total requests per week, high and low outliers
        3. the difference between last week and the week before
        4. request process time high and low outliers
    """

    # Retrieve data
    logging.info("Data is being read from {}".format(filename))
    df = pd.read_csv(filename, index_col=False)
    logging.info("Data Reading is done from {}. Performing outlier detection".format(filename))

    df.drop(columns=['location_address', 'location_city','location_state','location_zip'], inplace=True)

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
    pivot_yw = df.groupby(['cd', 'year_week', 'request_type']).size().unstack()
    pivot_yw['Total'] = pivot_yw.sum(axis=1)

    last_week = (datetime.today() - timedelta(7)).isocalendar()
    last_week_num = last_week[1]
    last_week_year = last_week[0]
    last_yw = '.'.join([str(last_week_year), str(last_week_num)])

    same_week_of_last_year = (datetime.today() - timedelta(days=365)).isocalendar()
    week_num_swoly = same_week_of_last_year[1]
    year_swoly = same_week_of_last_year[0]
    yw_a_year_ago = '.'.join([str(year_swoly), str(week_num_swoly)])


    # Get process time outliers
    df['process_time'] = (df['closed_datetime'] - df['created_datetime']).apply(lambda x: x.total_seconds()/3600)
    df_process_time = df.loc[:,['cd','srnumber', 'created_date', 'request_type', 'process_time']]
    df_process_time.dropna(inplace=True)

    # Get the first day of the week of the same day of the last year\
    same_day_of_last_year = (datetime.today() - relativedelta(years=1)).date()
    delta_to_first_day_of_that_week = same_day_of_last_year.isocalendar()[2] - 1
    first_day_of_last_yr_week = same_day_of_last_year - timedelta(days=delta_to_first_day_of_that_week)

    # Get the last day of last week
    one_week_ago = (datetime.today() - timedelta(days=7)).date()
    last_week_start = one_week_ago - timedelta(days=one_week_ago.weekday())
    last_week_end = last_week_start + timedelta(days=6)

    # Outlier Detection start from here
    idx = pd.IndexSlice
    cd_alert = {}

    for cd in cd_dict.keys():
        print('Council District: ' + str(int(cd)))

        df_last_whole_yr = pivot_yw.loc[idx[cd, yw_a_year_ago : last_yw], :].loc[idx[cd]]
        df_last_week = df_last_whole_yr.loc[last_yw]

        cols = df_last_whole_yr.columns
        cols = cols.drop('Total')

        # init the alert dict for each CD
        alert = {}

        # Get individual requet_type outliers
        for x in cols:
            flag = False
            if df_last_week[x] > df_last_whole_yr[x].mean() + 2 * df_last_whole_yr[x].std():
                alert[x] = [INDIV_HIGH_OUTLIER]
                flag = True
            elif df_last_week[x] < df_last_whole_yr[x].mean() - 2 * df_last_whole_yr[x].std():
                alert[x] = [INDIV_LOW_OUTLIER]    
                flag = True
            if flag:
                filename = make_save_graph(df_last_whole_yr, cd, x, 'Weekly Indiv Req Type Outlier')
                alert[x].append(filename)

        # Get weekly total outliers
        col = 'Total'
        high_bound = df_last_whole_yr[col].mean() + 2 * df_last_whole_yr[col].std()
        low_bound = df_last_whole_yr[col].mean() - 2 * df_last_whole_yr[col].std()
        flag = False
        if df_last_week[col] > high_bound:
            alert[col] = [TOTAL_HIGH_OUTLIER]
            flag = True
        elif df_last_week[col] < low_bound:
            alert[col] = [TOTAL_LOW_OUTLIER]
            flag = True
        if flag:
            filename = make_save_graph(df_last_whole_yr, cd, col, 'Weekly Total Outlier')
            alert[col].append(filename)

        # Get Diff of weekly total outliers
        pd.options.mode.chained_assignment = None  # turn off Value to be set on a copy of a slice warning
        df_last_whole_yr['Diff'] = df_last_whole_yr['Total'].diff()
        df_last_whole_yr['Diff'] = df_last_whole_yr['Diff'].abs()
        pd.options.mode.chained_assignment = "warn"

        col = 'Diff'
        high_bound = df_last_whole_yr[col].mean() + 2 * df_last_whole_yr[col].std()
        low_bound = df_last_whole_yr[col].mean() - 2 * df_last_whole_yr[col].std()
        flag = False
        if df_last_whole_yr[col][last_yw] > high_bound:
            alert[col] = [DIFF_HIGH_OUTLIER]
            flag = True
        elif df_last_whole_yr[col][last_yw] < low_bound:
            alert[col] = [DIFF_LOW_OUTLIER]
            flag = True
        if flag:
            filename = make_save_graph(df_last_whole_yr, cd, col, 'Weekly Diff Outlier')
            alert[col].append(filename)


        # Get Weekly Process Time outliers
        df_cd_proc_time = df_process_time[df_process_time.cd == cd]
        df_cd_proc_time = df_cd_proc_time[first_day_of_last_yr_week : last_week_end]
        df_proc_time_last_week = df_cd_proc_time.loc[last_week_start : last_week_end]
        
        for req_type in cols:
            df_temp = df_cd_proc_time[df_cd_proc_time.request_type == req_type]
            mean = df_temp['process_time'].mean()
            std = df_temp['process_time'].std()
            high_bound = mean + 2 * std
            low_bound = mean - 2 * std
            df_temp1 = df_proc_time_last_week[df_proc_time_last_week.request_type == req_type]
            for index, row in df_temp1.iterrows():
                flag = False

                if row['process_time'] > high_bound:
                    alert['srnumber: ' + row['srnumber']] = [HIGH_PROCESS_TIME_OUTLIER + ", " + 
                                row['request_type'] + ", " + str(row['process_time']) +" hrs"]
                    flag = True
                elif row['process_time'] < low_bound:
                    alert['srnumber: ' + row['srnumber']] = [LOW_PROCESS_TIME_OUTLIER + ", " + 
                                row['request_type'] + ", "  + str(row['process_time']) +" hrs"]
                    flag = True
                if flag:
                    filename = make_save_boxplot(df_temp['process_time'], cd, row['process_time'], req_type + ' SRNUMBER_' + row['srnumber'])
                    alert['srnumber: ' + row['srnumber']].append(filename)
        
        logging.info('Alert: ' + str(alert))
        cd_alert[cd] = alert

    return cd_alert


def sendemail(to_addr_list, cc_addr_list, subject,
              message, **kwargs):
    # xcom_pull alert from last task's return value
    task_instance = kwargs['task_instance']
    cd_alert = task_instance.xcom_pull(task_ids='detect_outliers')
              
    # if no outliers found, skip sending email
    if bool(cd_alert):
        # prepare email body
        for cd, alert in cd_alert.items():
            logging.info(alert)

            files = []
            for key, value in alert.items():
                files.append(prefix+value[1])

            html_content = make_html_content(cd, alert, message)
            send_email(to_addr_list[cd], subject, html_content,
                   files=files,  cc=cc_addr_list[cd])
        return "Outliers founded. Alert Email sent. Success!"

    return "No alert generated. Email not sent."

def make_html_content(cd, alert, message):
    count = 1
    c = (message.format(str(cd))).join(['<br><b>', '</b></br><br />'])
    for key, value in alert.items():
        c += "{}. {}:{}<br/>".format(str(count),value[0],key)
        count += 1
    return c


def remove_graph_png(**kwargs):
    task_instance = kwargs['task_instance']
    cd_alert = task_instance.xcom_pull(task_ids='detect_outliers')
    
    for cd, alert in cd_alert.items():
        for key, value in alert.items():
            file = value[1]
            if os.path.exists(prefix + file):
              os.remove(prefix + file)
            else:
              print("The file "+ file + " does not exist. Skipping to the next graph png")

sql_pull_data = \
    """
    COPY myla311_main TO '{}' DELIMITER ',' CSV HEADER;
    """

# airflow DAG arguments
args = {'owner': 'hunterowens',
    'start_date': airflow.utils.dates.days_ago(7),
    'provide_context': True,
    'email': ['hunter.owens@lacity.org','ITADATA@lacity.org'],
    'email_on_failure': False,
    'retries': 1, 
    'retry_delay': timedelta(minutes=5)
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
    postgres_conn_id='postgres_default',
    dag=dag)

task1 = PythonOperator(
    task_id='detect_outliers',
    provide_context=True,
    op_args=[filename],
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
    task_id='remove_graph_png',
    provide_context=True,
    python_callable=remove_graph_png,
    dag=dag)

task0 >> task1 >> task2 >> task3
