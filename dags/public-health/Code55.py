import requests
import sqlalchemy

from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

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

#Code to retrieve/clean data from each of the API endpoints
def get_code_55_df(url, datasetname):
    # make request to feature service url
    resp = requests.get(url)

    # store raw data into JSON format; isolate desired values from 'features' dict.keys()
    raw_data = resp.json()
    data = raw_data.get('features')
    # create new empty list to store desired final data
    cleandict = []
    # iterate through 'features' dict.items() which are nested dictionaries, isolate 'attributes' dict.items()
    # and append to empty list
    for x in range(len(data)):
        cleandict.append(data[x-1].get('attributes'))

    # load list of dictionaries into a pandas dataframe and convert to CSV file
    df = pd.DataFrame(cleandict)
    
    #Deduplicating DataFrame
    df["CompositeID"] = df["Number"].astype(str) + "-" + df["AssociatedServiceRequestNumber"]
    df.drop_duplicates(subset = "CompositeID", inplace = True)
    
    #Assigning dataset
    df["DataSetName"] = datasetname
    
    #Adjusting datetimes from unix timestamp to readable date
    datelist = ['DateCreated', 'DateApproved', 'ExpirationDate', 'DateCompleted', 'CleanupDate']
    for date in datelist:   
        df[date] = pd.to_datetime(df[date], unit = "ms")
    return df

def update_code_55():
    #Collecting each dataset
    code_55_closed_last_90 = get_code_55_df(r'https://production.sanstarla.com/FeatureService/REST/services/Authorizations/FeatureServer/0/query', "Closed")
    code_55_pending_schedule = get_code_55_df(r'https://production.sanstarla.com/FeatureService/REST/services/Authorizations/FeatureServer/1/query', "Pending Scheduled")
    code_55_scheduled = get_code_55_df(r'https://production.sanstarla.com/FeatureService/REST/services/Authorizations/FeatureServer/2/query', "Scheduled")
    
    #Prioritizing data where Closed Last 90 > Scheduled > Pending Schedule
    code_55_pending_schedule = code_55_pending_schedule.loc[~(code_55_pending_schedule["CompositeID"].isin(code_55_closed_last_90["CompositeID"]))&~(code_55_pending_schedule["CompositeID"].isin(code_55_scheduled["CompositeID"]))]
    code_55_scheduled = code_55_scheduled.loc[~code_55_scheduled["CompositeID"].isin(code_55_closed_last_90["CompositeID"])]
    
    #Merging all data into one dataframe
    df = pd.concat([code_55_closed_last_90, code_55_pending_schedule, code_55_scheduled])
    
    #Deleting old records
    engine = PostgresHook.get_hook("postgres_default").get_sqlalchemy_engine()
    object_id_list = ",".join(list(df["CompositeID"].astype(str)))
    inspector = sqlalchemy.inspect(engine)
    schemas = inspector.get_schema_names()
    for schema in schemas:
        if schema == "public-health":
            if "Code55s" in inspector.get_table_names(schema=schema):
                engine.connect().execute('DELETE FROM "public-health".Code75s WHERE CompositeID IN (%s)' % object_id_list)
                break
    
    #Sending updates and new records to postgres
    df.to_sql(
        "Code55s",
        engine,
        schema="public-health",
        if_exists="append",
    ) 

t1 = PythonOperator(
    task_id="Update_Code_55_Data",
    provide_context=True,
    python_callable=update_code_55,
    dag=dag,
)