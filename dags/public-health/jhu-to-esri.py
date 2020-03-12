import pandas as pd 
import geopandas as gpd
from arcgis.gis import GIS
from airflow.hooks.base_hook import BaseHook

DEATHS_URL = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Deaths.csv"

CASES_URL = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Confirmed.csv"

def load_jhu_to_esri(**kwargs): 
    """
    A single python ETL function. 

    Loads the JHU data, transforms it so we are happy 
    with it, and then creates the appropriate featureLayer

    Ref: 
    """
    pass

    cases = pd.read_csv(CASES_URL)

    us_cases = cases[cases['Country/Region'] == 'US']   

    columns = list(us_cases.columns)

    id_vars, dates = [], []

    for c in columns:
        if c.endswith('20'):
            dates.append(c)
        else:
            id_vars.append(c)

    df = pd.melt(us_cases, id_vars=id_vars, value_vars=dates, value_name='number_of_cases')
    gdf = geopandas.GeoDataFrame(df, geometry=geopandas.points_from_xy(df.Longitude, df.Latitude))
    gdf.to_file('/tmp/jhu-dataset.shp')

    # Login to ArcGIS
    arcconnection = BaseHook.get_connection("arcgis")
    arcuser = arcconnection.login
    arcpassword = arcconnection.password
    gis = GIS('http://lahub.maps.arcgis.com', username=arcuser, password=arcpassword)
    



default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020-, 3, 1),
    "email": [
        "ian.rose@lacity.org",
        "hunter.owens@lacity.org",
    ],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(days=1),
}

dag = DAG("jhu-to-esri", default_args=default_args, schedule_interval="@daily")


# Sync ServiceRequestData.csv
t1 = PythonOperator(
    task_id="sync-jhu-to-esri",
    provide_context=True,
    python_callable=update_rap_data,
    op_kwargs={
        "arcfeatureid": "", ## fix 
    },
    dag=dag,
)