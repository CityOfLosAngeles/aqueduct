from datetime import datetime, timedelta

import pandas as pd
import requests

from airflow import DAG
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

dag = DAG("code-55", default_args=default_args, schedule_interval="@daily")


# Code to retrieve/clean data from each of the API endpoints
def get_code_55_df(urlnumber, stage):
    # make request to feature service url
    url = (
        r"https://production.sanstarla.com/FeatureService/REST/services/Authorizations/FeatureServer/%s/query" % urlnumber
    )
    resp = requests.get(url)

    # store raw data into JSON format
    # isolate desired values from 'features' dict.keys()
    raw_data = resp.json()
    data = raw_data.get("features")
    # create new empty list to store desired final data
    cleandict = []
    # iterate through 'features' dict.items() which are nested dictionaries
    # isolate 'attributes' dict.items() and append to empty list
    for d in data:
        cleandict.append(d.get("attributes"))

    # load list of dictionaries into a pandas dataframe and convert to CSV file
    df = pd.DataFrame(cleandict)

    # Deduplicating DataFrame
    df["CompositeID"] = (
        df["Number"].astype(str) + "-" + df["AssociatedServiceRequestNumber"]
    )
    df.drop_duplicates(subset="CompositeID", inplace=True)

    # Assigning dataset
    df["Stage"] = stage

    # Adjusting datetimes from unix timestamp to readable date
    datelist = [
        "DateCreated",
        "DateApproved",
        "ExpirationDate",
        "DateCompleted",
        "CleanupDate",
    ]
    for date in datelist:
        df[date] = pd.to_datetime(df[date], unit="ms")
    return df


def update_code_55():
    # Collecting each dataset
    code_55_closed_last_90 = get_code_55_df(0, "Closed")
    code_55_pending_schedule = get_code_55_df(1, "Pending Scheduled")
    code_55_scheduled = get_code_55_df(2, "Scheduled")

    # Prioritizing data where Closed Last 90 > Scheduled > Pending Schedule
    code_55_pending_schedule = code_55_pending_schedule.loc[
        ~(
            code_55_pending_schedule["CompositeID"].isin(
                code_55_closed_last_90["CompositeID"]
            )
        )
        & ~(
            code_55_pending_schedule["CompositeID"].isin(
                code_55_scheduled["CompositeID"]
            )
        )
    ]
    code_55_scheduled = code_55_scheduled.loc[
        ~code_55_scheduled["CompositeID"].isin(code_55_closed_last_90["CompositeID"])
    ]

    # Merging all data into one dataframe
    df = pd.concat(
        [code_55_closed_last_90, code_55_pending_schedule, code_55_scheduled]
    )

    # Creating table if does not exist
    engine = PostgresHook.get_hook("postgres_default").get_sqlalchemy_engine()
    create_table_statement = """CREATE TABLE IF NOT EXISTS "public-health"."code55s" (
    index BIGINT,
    "Id" BIGINT,
    "GlobalID" TEXT,
    "Number" TEXT,
    "AssociatedServiceRequestNumber" TEXT,
    "DateCreated" TIMESTAMP WITHOUT TIME ZONE,
    "DateApproved" TIMESTAMP WITHOUT TIME ZONE,
    "ExpirationDate" TIMESTAMP WITHOUT TIME ZONE,
    "Status" BIGINT,
    "Address" TEXT,
    "City" TEXT,
    "ZipCode" TEXT,
    "CrossStreet" TEXT,
    "CouncilDistrict" TEXT,
    "APREC" TEXT,
    "LocationComments" TEXT,
    "SubmittedBy" TEXT,
    "ReportingPerson" TEXT,
    "RPContactNo" TEXT,
    "Details" TEXT,
    "HEAssessmentBy" TEXT,
    "AssessmentContactNo" TEXT,
    "HEAssessmentDetails" TEXT,
    "AssessmentLocationDescription" TEXT,
    "AnchorPhotos" TEXT,
    "DateCompleted" TIMESTAMP WITHOUT TIME ZONE,
    "CleanupDate" TIMESTAMP WITHOUT TIME ZONE,
    "CompositeID" TEXT,
    "Stage" TEXT
    )"""
    engine.connect().execute(create_table_statement)

    # Deleting old records
    object_id_list = ",".join(list(df["CompositeID"].astype(str)))
    engine.connect().execute(
        'DELETE FROM "public-health".code55s WHERE CompositeID IN (%s)' % object_id_list
    )

    # Sending updates and new records to postgres
    df.to_sql(
        "code55s", engine, schema="public-health", if_exists="append",
    )


t1 = PythonOperator(
    task_id="Update_Code_55_Data",
    provide_context=True,
    python_callable=update_code_55,
    dag=dag,
)
