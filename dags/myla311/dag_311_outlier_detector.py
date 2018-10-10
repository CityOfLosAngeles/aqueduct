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
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from config import *
import matplotlib.pyplot as plt
from matplotlib import *
import seaborn as sns
import altair as alt

# a csv file with this filename will be saved from a copy of postgres table
filename = "./myla311.csv"

# email parameters
message = "This last week's Outliers:"
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


def make_save_graph(df, col, title):
    line = alt.Chart(df.reset_index(), title = ' - '.join([title, col])).mark_line().encode(
    x='year_week:O',
    y= col+':Q')
    rule = alt.Chart(df).mark_rule(color='red').encode(
    alt.Y('average({})'.format(col)),
    size=alt.value(1))
    graph = line + rule
    filename = 'chart-{}.png'.format(col.replace('/', '-').replace(' ','-'))
    graph.save(filename)
    return filename


def make_save_boxplot(df,point,title):
    # using seaborn
    sns.set_style("whitegrid")
    fig, ax = plt.subplots(figsize=(8,8))
    ax.set_title(title, fontsize=15)
    plot1 = sns.boxplot(ax=ax, x=df, linewidth=1, color='lightblue')
    plot2 = plt.scatter(point, 0, marker='o', s=100, c='red', linewidths=5,label='Outlier')
    ax.legend()
    filename = 'chart-Proc-Time-{}.png'.format(title.replace('/', '-').replace(' ','-'))
    plt.savefig(filename)
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
    cols = cols.drop('Total')
    # make the alert dict
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
            filename = make_save_graph(df_last_whole_yr, x, 'Indiv Req Type Outlier')
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
        filename = make_save_graph(df_last_whole_yr, col, col+' Outlier')
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
        filename = make_save_graph(df_last_whole_yr, col, 'Weekly Diff Outlier')
        alert[col].append(filename)

    # Get process time outliers
    df['process_time'] = (df['closed_datetime'] - df['created_datetime']).apply(lambda x: x.total_seconds()/3600)
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

    for req_type in cols:
        df_temp = df_process_time[df_process_time.request_type == req_type]
        mean = df_temp['process_time'].mean()
        std = df_temp['process_time'].std()
        high_bound = mean + 2 * std
        low_bound = mean - 2 * std
        df_temp1 = df_proc_time_last_week[df_proc_time_last_week.request_type == req_type]
        for index, row in df_temp1.iterrows():
            flag = False
           
            if row['process_time'] > high_bound:
                alert['srnumber: ' + row['srnumber']] = [HIGH_PROCESS_TIME_OUTLIER + ", " + row['request_type'] + ", "                                       + str(row['process_time']) +" hrs"]
                flag = True
            elif row['process_time'] < low_bound:
                alert['srnumber: ' + row['srnumber']] = [LOW_PROCESS_TIME_OUTLIER + ", " + row['request_type'] + ", "                                       + str(row['process_time']) +" hrs"]
                flag = True
            if flag:
                filename = make_save_boxplot(df_temp['process_time'], row['process_time'], req_type + ' SRNUMBER:' + row['srnumber'])
                alert['srnumber: ' + row['srnumber']].append(filename)

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

        #************************************************************************
        msgRoot = MIMEMultipart('related')
        msgRoot['Subject'] = subject
        msgRoot['From'] = from_addr
        msgRoot['To'] = to_addr_list
        msgRoot.preamble = 'This is a multi-part message in MIME format.'

        # Encapsulate the plain and HTML versions of the message body in an
        # 'alternative' part, so message agents can decide which they want to display.
        msgAlternative = MIMEMultipart('alternative')
        msgRoot.attach(msgAlternative)

        msgText = MIMEText('Please review this email on a computer in order to read HTML content')
        msgAlternative.attach(msgText)

        # We reference the image in the IMG SRC attribute by the ID we give it below
        html_content = make_html_content(alert, message)

        msgText1 = MIMEText(html_content, 'html')
        msgAlternative.attach(msgText1)
        
        # This example assumes the image is in the current directory
        count=1
        for key, value in alert.items():
            fp = open(value[1], 'rb')
            msgImage = MIMEImage(fp.read())
            fp.close()
            msgImage.add_header('Content-ID', '<image'+ str(count) +'>')
            msgRoot.attach(msgImage)
            count += 1
        #************************************************************************
        server = smtplib.SMTP(smtpserver)
        server.starttls()
        server.login(login, password)
        server.sendmail(from_addr, [to_addr_list, cc_addr_list], msgRoot.as_string())
        server.quit()
        return "Outliers founded. Alert Email sent. Success!"

    return "No alert generated. Email not sent."

def make_html_content(alert, message):
    count = 1
    c = message.join(['<br><b>', '</b></br><br />'])
    for key, value in alert.items():
        c += str(count) + '. ' + value[0] + ': ' + key
        c += '<br><img src="cid:image' + str(count) + '"><br>'
        count += 1
    return c


def remove_graph_png(**kwargs):
    task_instance = kwargs['task_instance']
    alert = task_instance.xcom_pull(task_ids='detect_outliers')
    alert = {'Illegal Dumping Pickup': ['HIGH INDIVIDUAL REQUEST TYPE OUTLIER',
          'chart-Illegal-Dumping-Pickup.png'],
         'Illegal Dumping in Progress': ['HIGH INDIVIDUAL REQUEST TYPE OUTLIER',
          'chart-Illegal-Dumping-in-Progress.png'],
         'Median Island Maintenance': ['HIGH INDIVIDUAL REQUEST TYPE OUTLIER',
          'chart-Median-Island-Maintenance.png'],
         'Park Fields/Buildings/Repairs': ['HIGH INDIVIDUAL REQUEST TYPE OUTLIER',
          'chart-Park-Fields-Buildings-Repairs.png'],
         'Park Graffiti/Trash': ['HIGH INDIVIDUAL REQUEST TYPE OUTLIER',
          'chart-Park-Graffiti-Trash.png'],
         'Park Homelessness and Security': ['HIGH INDIVIDUAL REQUEST TYPE OUTLIER',
          'chart-Park-Homelessness-and-Security.png'],
         'Tables and Chairs Obstructing': ['HIGH INDIVIDUAL REQUEST TYPE OUTLIER',
          'chart-Tables-and-Chairs-Obstructing.png'],
         'srnumber: 1-1197288301': ['HIGH PROCESS TIME OUTLIER, Dead Animal Removal, 44.53527777777778 hrs',
          'chart-Proc-Time-Dead-Animal-Removal-SRNUMBER:1-1197288301.png'],
         'srnumber: 1-1197458141': ['HIGH PROCESS TIME OUTLIER, Dead Animal Removal, 46.02111111111111 hrs',
          'chart-Proc-Time-Dead-Animal-Removal-SRNUMBER:1-1197458141.png']}
    
    for key, value in alert.items():
        file = value[1]
        if os.path.exists(file):
          os.remove(file)
        else:
          print("The file "+ file + " does not exist. Skipping to the next graph png")

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
    op_args=[filename],
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

task3 = PythonOperator(
    task_id='remove_graph_png',
    provide_context=True,
    python_callable=remove_graph_png,
    dag=dag)

task0 >> task1 >> task2 >> task3
