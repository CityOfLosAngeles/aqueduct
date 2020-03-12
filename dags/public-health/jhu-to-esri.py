import pandas as pd 
import geopandas as gpd
from arcgis.gis import GIS
from airflow.hooks.base_hook import BaseHook

DEATHS_URL = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Deaths.csv"

CASES_URL = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Confirmed.csv"

RECOVERED_URL = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Recovered.csv"

def parse_columns(df): 
    """
    quick helper function to parse columns into values 
    uses for pd.melt
    """

    columns = list(df.columns)

    id_vars, dates = [], []

    for c in columns:
        if c.endswith('20'):
            dates.append(c)
        else:
            id_vars.append(c)
    return id_vars, dates

def load_jhu_to_esri(**kwargs): 
    """
    A single python ETL function. 

    Loads the JHU data, transforms it so we are happy 
    with it, and then creates the appropriate featureLayer

    Ref: 
    """
    cases = pd.read_csv(CASES_URL)
    deaths = pd.read_csv(DEATHS_URL)
    recovered = pd.read_csv(RECOVERED_URL)
    # melt cases 
    id_vars, dates = parse_columns(cases)
    df = pd.melt(cases,
                 id_vars=id_vars, 
                 value_vars=dates, 
                 value_name='number_of_cases', 
                 var_name='date')

    # melt deaths
    id_vars, dates = parse_columns(deaths) 
    deaths_df = pd.melt(deaths, id_vars=id_vars, value_vars=dates, value_name='number_of_deaths')

    # melt recovered 

    id_vars, dates = parse_columns(deaths) 
    recovered_df = pd.melt(recovered, id_vars=id_vars, value_vars=dates, value_name='number_of_recovered')

    # join
    df['number_of_deaths'] = deaths_df.number_of_deaths
    df['number_of_recovered'] = recovered_df.number_of_recovered
    # make some simple asserts to assure that the data structures haven't changed and some old numbers 
    # are still correct 
    assert df.loc[(df['Province/State'] == 'Los Angeles, CA') & (df['date'] == '3/11/20')]['number_of_cases'].item() == 27
    assert df.loc[(df['Province/State'] == 'Los Angeles, CA') & (df['date'] == '3/11/20')]['number_of_deaths'].item() == 1
    assert df.loc[(df['Province/State'] == 'Shanghai') & (df['date'] == '3/11/20')]['number_of_recovered'].item() == 320
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
    "start_date": datetime(2020, 3, 1),
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