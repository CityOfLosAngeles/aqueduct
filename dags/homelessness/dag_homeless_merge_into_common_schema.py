import airflow
import logging
import pandas as pd
import numpy as np
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
import os

filename = '/tmp/homeless.csv'


def merge_and_save(**kwargs):
    # read the census tract files of each year
    df2018 = pd.read_excel('/tmp/homeless2018.xlsx', sheet_name='Counts_by_Tract')
    df2017 = pd.read_excel('/tmp/homeless2017.xlsx', sheet_name='Count_by_Tract')
    df2016 = pd.read_excel('/tmp/homeless2016.xlsx', sheet_name='Tracts')
    df2015 = pd.read_excel('/tmp/homeless2015.xlsx', sheet_name='Update 07 27 15')

    # make a useful list
    lst = [df2015, df2016, df2017, df2018]

    # round float column to int
    for df in lst:
        for col in df.columns:
            if df[col].dtype==np.float64:
                df[col] = df[col].round(0).astype(int)

    # read the common schema
    df_schema = pd.read_excel('/tmp/common_schema.xlsx', sheet_name='Sheet1')

    # read the list of columns to be dropped
    droplist = pd.read_excel('/tmp/common_schema.xlsx', sheet_name='drop_list')

    # change column names to lowercase
    for df in lst:
        df.columns = df.columns.str.lower()

    droplist.sort_values(by='year', inplace=True)

    # dropping
    df2018.name, df2017.name, df2016.name, df2015.name = 2018, 2017, 2016, 2015 

    for df in lst:
        df.drop(columns=droplist[droplist.year==df.name].col_name, inplace=True)

    # 1. Applying the Common Scheme to each year's table
    # 2. Combine them into one table
    df_schema.drop(columns=['Index', 'note', 'Description'], inplace=True)
    df_schema.rename(columns={'Combined Column Name':'comb_col_name', 'Common column name':'common_col_name','Description':'description',
                              'column name':'col_name', 'year':'year'}, inplace=True)

    # making replace_col_dict and replace col in each ear
    # in order to make the cols in all years more readable and consistent
    for df in lst:
        logging.info("Applying Common Schema on {}".format(df.name))
        df_this_year_schema = df_schema[df_schema.year==df.name]
        df_this_year_schema.reset_index(drop=True,inplace=True)
        keys, values = df_this_year_schema.col_name, df_this_year_schema.common_col_name
        replace_col_dict = {k: v for k, v in zip(keys, values)}
        df.rename(columns = replace_col_dict, inplace=True)

    # Taking care of a special case (refer to the 'Combined Column Name' in the Common Schema file)
    df_special = df_schema[df_schema.comb_col_name.notnull()]
    df2017['tot_tent_people'] = df2017['fam_tent_people'] + df2017['fam_tent_hh']
    df2018['tot_tent_people'] = df2018['fam_tent_people'] + df2018['fam_tent_hh']

    # Taking care of another Speicial Case - inconsistent spa data format:
    df2015.spa = df2015.spa.apply(lambda x: x[4:])

    # df2015.spa.unique()
    # array(['2', '7', '4', '3', '0', '5', '6', '8', '1'], dtype=object)
    # Note that the '0' above is a NaN data point, we don't have a spa code of '0'

    # Appending the dataframes
    df_all = df2018.append(df2017, sort=False).append(df2016, sort=False).append(df2015, sort=False)

    df_all.rename(columns={'2015total_woyouth':'total_woyouth2015'}, inplace=True)
    # saving
    df_all.to_csv('/tmp/homeless.csv', index=False)

#sql commands
sql_drop_main = \
    """
    DROP TABLE IF EXISTS homeless_main;
    """

sql_create_main = \
    """
    CREATE TABLE homeless_main
    (
        count_year                  text,
        tract_code_2010             text,
        tract_code_2000             text,
        city                        text,
        lacity                      text,
        community_name              text,
        detailed_name               text,
        spa                         text,
        sd                          text,
        us_cd                       text,
        demog_surv_ct               text,
        youth_surv_ct               text,
        shelt_hmis_ct               text,
        shelter_count_any           text,
        street_count_any            text,
        tot_street_sing_adult       text,
        tot_street_fam_hh           text,
        tot_street_fam_mem          text,
        tot_cars                    text,
        tot_vans                    text,
        tot_campers                 text,
        tot_tents                   text,
        tot_makeshift               text,
        tot_car_people              text,
        tot_van_people              text,
        tot_camper_people           text,
        tot_tent_people             text,
        tot_makeshift_people        text,
        fam_car_hh                  text,
        fam_van_hh                  text,
        fam_camper_hh               text,
        fam_tent_hh                 text,
        fam_makeshift_hh            text,
        fam_car_people              text,
        fam_van_people              text,
        fam_camper_people           text,
        fam_tent_people             text,
        fam_makeshift_people        text,
        ind_car_people              text,
        tot_van_ind_people          text,
        ind_camper_people           text,
        ind_tent_people             text,
        ind_makeshift_people        text,
        tot_es_adult_sing_adult     text,
        tot_es_adult_famhh          text,
        tot_es_adult_fam_mem        text,
        tot_es_youth_sing_youth     text,
        tot_es_youth_fam_hh         text,
        tot_es_youth_fam_mem        text,
        tot_es_youth_unacc_youth    text,
        tot_th_adult_singadult      text,
        tot_th_adult_fam_hh         text,
        tot_th_adult_fam_mem        text,
        tot_th_youth_sing_youth     text,
        tot_th_youth_fam_hh         text,
        tot_th_youth_fam_mem        text,
        tot_th_youth_unacc_youth    text,
        tot_sh_adult_sing_adult     text,
        tot_sh_adult_fam_hh         text,
        tot_sh_adult_fam_mem        text,
        tot_sh_youth_sing_youth     text,
        tot_sh_youth_fam_hh         text,
        tot_sh_youth_fam_mem        text,
        tot_sh_youth_unaccyouth     text,
        tot_unshelt_people          text,
        tot_es_people               text,
        tot_th_people               text,
        tot_sh_people               text,
        tot_shelt_people            text,
        tot_people                  text,
        uscd                        text,
        demog_survey_sample         text,
        youth_count_sample          text,
        sqmi                        text,
        community_type              text,
        street_sing                 text,
        fam_street_people           text,
        street_unacc_minor          text,
        tot_unit                    text,
        youth_count_15              text,
        youthcount13                text,
        tot_es_single_ind           text,
        fam_es_mem                  text,
        sing_adult_th               text,
        fam_shelter_mem             text,
        tot_street_count            text,
        tot_unshelter               text,
        tot_th                      text,
        tot_shelter                 text,
        tot_ind_adult               text,
        tot_fam_mem                 text,
        tot_unacc_minor             text,
        total_woyouth2015           text
    );
    """

sql_insert_into_main = \
    """
    COPY homeless_main
    FROM '{}' WITH CSV HEADER delimiter ',';
    """.format(filename)


# airflow DAG arguments
args = {
    'owner': 'hunterowens',
    'start_date': airflow.utils.dates.days_ago(7),
    'provide_context': True,
    'email': ['hunter.owens@lacity.org'],
    'email_on_failure': False,
    'retries': 1, 
    'retry_delay': timedelta(minutes=5)
    }

# initiating the DAG
dag = airflow.DAG(
    'merge_homeless_datasets_into_one',
    schedule_interval="@monthly",
    default_args=args,
    max_active_runs=1)

# when future annual datasets becomes available, just add a new url and add it to the urls list
# the path & filename after '-O' is the path & name that the files are to be placed and renamed to
url2018 = 'wget -O /tmp/homeless2018.xlsx https://github.com/CityOfLosAngeles/aqueduct/raw/master/dags/homelessness/static_datasets/homeless2018.xlsx'
url2017 = 'wget -O /tmp/homeless2017.xlsx https://github.com/CityOfLosAngeles/aqueduct/raw/master/dags/homelessness/static_datasets/homeless2017.xlsx'
url2016 = 'wget -O /tmp/homeless2016.xlsx https://github.com/CityOfLosAngeles/aqueduct/raw/master/dags/homelessness/static_datasets/homeless2016.xlsx'
url2015 = 'wget -O /tmp/homeless2015.xlsx https://github.com/CityOfLosAngeles/aqueduct/raw/master/dags/homelessness/static_datasets/homeless2015.xlsx'
common_schema = 'wget -O /tmp/common_schema.xlsx https://github.com/CityOfLosAngeles/aqueduct/raw/master/dags/homelessness/static_datasets/common_schema_v1.xlsx'
urls = [url2018, url2017, url2016, url2015, common_schema]

# bash command to download all datasets in urls list
download_command = ""
for index, url in enumerate(urls):
    if index==len(urls)-1:
        download_command += url
    else:
        download_command += url + " && "

# make remove list of the staging files
rm_list = [url.split()[2] for url in urls]
rm_list.append(filename)

rm_command = "rm -f "
for file in rm_list:
    rm_command += file + " "

# creating tasks
task0 = BashOperator(
    task_id='retrieve_data',
    bash_command=download_command,
    dag=dag
    )

task1 = PostgresOperator(
    task_id='drop_main_table_if_exist',
    sql=sql_drop_main,
    postgres_conn_id='postgres_default',
    dag=dag
    )

task2 = PostgresOperator(
    task_id='create_main_table',
    sql=sql_create_main,
    postgres_conn_id='postgres_default',
    dag=dag
    )

task3 = PythonOperator(
    task_id='merge_into_common_schema',
    provide_context=True,
    python_callable=merge_and_save,
    dag=dag
    )

task4 = PostgresOperator(
    task_id='insert_into_main_table',
    sql=sql_insert_into_main,
    postgres_conn_id='postgres_default',
    dag=dag
    )

task5 = BashOperator(
    task_id='remove_tmp_files',
    bash_command=rm_command,
    dag=dag
    )

# task sequence
task0 >> task1 >> task2 >> task3 >> task4 >> task5
