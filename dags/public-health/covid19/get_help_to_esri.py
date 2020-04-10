import datetime
from urllib.parse import urljoin

import pandas
import requests
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from arcgis.gis import GIS

API_BASE_URL = "https://api2.gethelp.com/v1/"

FACILITIES_ID = "abc123"

TIMESERIES_ID = "abc123"


def upload_to_esri(df, layer_id, filename="/tmp/df.csv"):
    """
    A quick helper function to upload a data frame
    to ESRI as a featurelayer backed CSV

    recommend: no geometries, lat/long columns
    remember ESRI is UTC only.
    """
    df.to_csv(filename, index=False)
    # Login to ArcGIS
    arcconnection = BaseHook.get_connection("arcgis")
    arcuser = arcconnection.login
    arcpassword = arcconnection.password
    gis = GIS("http://lahub.maps.arcgis.com", username=arcuser, password=arcpassword)

    # gis_item = gis.content.get(layer_id)
    # gis_layer_collection = arcgis.features.FeatureLayerCollection.fromitem(gis_item)
    # gis_layer_collection.manager.overwrite(filename)

    # os.remove(filename)
    return True


def make_get_help_request(api_path, token, params={}):
    """
    Makes an API request to the GetHelp platform.
    Also handles depagination of long responses.

    Parameters
    ==========
    api_path: string
        The path to query
    token: string
        The OAuth bearer token
    params: dict
        Any additional query parameters to pass

    Returns
    =======
    The depaginated JSON response in the "content" field.
    """
    endpoint = urljoin(API_BASE_URL, api_path)
    content = []
    page = 0
    while True:
        r = requests.get(
            endpoint,
            headers={"Authorization": f"Bearer {token}"},
            params=dict(page=page, **params),
        )
        res = r.json()
        content = content + res["content"]
        if res["last"] is True:
            break
        else:
            page = page + 1

    return content


def get_facilities():
    """
    Get the current facilties and their status.

    Returns
    =======
    A dataframe with the current facilities.
    """
    TOKEN = Variable.get("GETHELP_OAUTH_PASSWORD")
    res = make_get_help_request("facility-groups/1/facilities", TOKEN)
    return pandas.io.json.json_normalize(res)


def get_facility_history(facility_id, start_date=None, end_date=None):
    """
    Get the history stats of a given facility by ID.

    Parameters
    ==========
    facility_id: int
        The ID of the facility.
    start_date: datetime.date
        The start date of the history (defaults to April 8, 2020)
    end_date: datetme.date
        The end date of the history (defaults to the present day)

    Returns
    =======
    A dataframe with the history for the given facility.
    """
    TOKEN = Variable.get("GETHELP_OAUTH_PASSWORD")
    start_date = start_date or datetime.date(2020, 4, 8)
    end_date = end_date or pandas.Timestamp.now(tz="US/Pacific").date()

    # Get the shelter bed program ID
    res = make_get_help_request(f"facilities/{facility_id}/facility-programs", TOKEN)
    programs = pandas.io.json.json_normalize(res)
    assert len(programs)
    shelter_programs = programs[programs.name.str.lower().str.contains("shelter bed")]
    assert len(shelter_programs) == 1
    program_id = shelter_programs.iloc[0]["id"]

    # Get the history stats for the shelter bed program
    res = make_get_help_request(
        f"facilities/{facility_id}/facility-programs/{program_id}/statistics",
        TOKEN,
        params={"startDate": str(start_date), "endDate": str(end_date)},
    )
    history = pandas.io.json.json_normalize(res)

    # Add ID column so we can filter by them later
    history = history.assign(program_id=program_id,)
    return history


def assemble_facility_history(facility):
    """
    Given a facility, assemble its history stats into a dataframe.
    This is the same as get_facility_history, but also adds some
    additional columns from the facility data.

    Parameters
    ==========
    facility: pandas.Series
        A row from the facilities dataframe

    Returns
    =======
    A dataframe with facility history.
    """
    history = get_facility_history(facility["id"])
    if not len(history):
        return None
    history = history.assign(
        facility_id=facility["id"],
        name=facility["name"],
        phone=facility["phone"],
        website=facility["website"],
        address=facility["address1"],
        city=facility["city"],
        county=facility["county"],
        state=facility["state"],
        zipCode=facility["zipCode"],
        latitude=facility["latitude"],
        longitude=facility["longitude"],
    ).drop(columns=["id"])
    return history


def assemble_get_help_timeseries():
    """
    Gets a full timeseries for all facilities managed by the GetHelp system.
    """
    df = pandas.DataFrame()
    facilities = get_facilities()
    for idx, facility in facilities.iterrows():
        history = assemble_facility_history(facility)
        if history is not None:
            df = df.append(history)
    df = df.assign(
        dataDate=pandas.to_datetime(df.dataDate)
        .dt.tz_localize("US/Pacific")
        .dt.tz_convert("UTC")
    ).sort_values(["facility_id", "dataDate"])
    return df


def load_get_help_data(**kwargs):
    facilities = get_facilities()
    upload_to_esri(facilities, FACILITIES_ID, "/tmp/facilities.csv")
    timeseries = assemble_get_help_timeseries()
    upload_to_esri(timeseries, TIMESERIES_ID, "/tmp/timeseries.csv")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.datetime(2020, 4, 10),
    "email": ["ian.rose@lacity.org", "hunter.owens@lacity.org", "itadata@lacity.org"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
}

dag = DAG(
    "get-help-to-esri", default_args=default_args, schedule_interval="*/15 * * * *"
)


t1 = PythonOperator(
    task_id="load_get_help_data",
    provide_context=False,
    python_callable=load_get_help_data,
    op_kwargs={},
    dag=dag,
)
