import datetime
import functools
import os
from urllib.parse import urljoin

import arcgis
import geopandas
import numpy
import pandas
import requests
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.utils.email import send_email
from arcgis.gis import GIS

API_BASE_URL = "https://api2.gethelp.com/v1/"

FACILITIES_ID = "63e3696eddc94262a472d8c99a58780c"

STATS_ID = "9db2e26c98134fae9a6f5c154a1e9ac9"

TIMESERIES_ID = "bd17014f8a954681be8c383acdb6c808"

COUNCIL_DISTRICTS = "https://opendata.arcgis.com/datasets/76104f230e384f38871eb3c4782f903d_13.geojson"  # noqa: E501


def download_council_districts():
    r = requests.get(COUNCIL_DISTRICTS)
    fname = "/tmp/council-districts.geojson"
    with open(fname, "wb") as f:
        f.write(r.content)
    return fname


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

    gis_item = gis.content.get(layer_id)
    gis_layer_collection = arcgis.features.FeatureLayerCollection.fromitem(gis_item)
    gis_layer_collection.manager.overwrite(filename)

    os.remove(filename)
    return True


def make_get_help_request(api_path, token, params={}, paginated=True):
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
    paginated: boolean
        Whether the response is expected to be a list of paginated results
        with a "content" field. In this case, the function will depaginate
        the results. If false, it will return the raw JSON.

    Returns
    =======
    The depaginated JSON response in the "content" field, or the raw JSON response.
    """
    endpoint = urljoin(API_BASE_URL, api_path)
    if paginated:
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
    else:
        r = requests.get(
            endpoint, headers={"Authorization": f"Bearer {token}"}, params=params,
        )
        return r.json()


def get_facilities():
    """
    Get the current facilties and their status.

    Returns
    =======
    A dataframe with the current facilities.
    """
    TOKEN = Variable.get("GETHELP_OAUTH_PASSWORD")
    res = make_get_help_request("facility-groups/1/facilities", TOKEN)
    df = pandas.io.json.json_normalize(res)
    df = pandas.concat(
        [df, df.apply(lambda x: get_client_stats(x["id"]), axis=1)], axis=1,
    )
    df = pandas.concat(
        [df, df.apply(lambda x: get_facility_program_status(x["id"]), axis=1)], axis=1,
    )
    council_districts = geopandas.read_file(
        download_council_districts(), driver="GeoJSON"
    )[["geometry", "District"]]
    df = geopandas.GeoDataFrame(
        df,
        geometry=geopandas.points_from_xy(df.longitude, df.latitude),
        crs={"init": "epsg:4326"},
    )
    df = df.assign(
        district=df.apply(
            lambda x: council_districts[council_districts.contains(x.geometry)]
            .iloc[0]
            .District,
            axis=1,
        )
    ).drop(columns=["geometry"])
    return df


def get_client_stats(facility_id):
    """
    Given a facility ID, get the current client status.

    Parameters
    ==========

    facility_id: int
        The facility ID

    Returns
    =======

    A pandas.Series with the client statistics for the facility.
    """
    TOKEN = Variable.get("GETHELP_OAUTH_PASSWORD")
    res = make_get_help_request(
        f"facilities/{facility_id}/client-statistics", TOKEN, paginated=False,
    )
    return (
        pandas.Series({**res, **res["genderStats"], **res["clientEvents"]})
        .drop(["genderStats", "clientEvents"])
        .astype(int)
    )


def get_program_client_stats(facility_id, program_id):
    """
    Given a facility ID and a program ID, get the current client status.

    Parameters
    ==========

    facility_id: int
        The facility ID

    program_id: int
        The program ID

    Returns
    =======

    A pandas.Series with the client statistics for the facility program.
    """
    TOKEN = Variable.get("GETHELP_OAUTH_PASSWORD")
    res = make_get_help_request(
        f"facilities/{facility_id}/facility-programs/{program_id}/client-statistics",
        TOKEN,
        paginated=False,
    )
    return (
        pandas.Series({**res, **res["genderStats"], **res["clientEvents"]})
        .drop(["genderStats", "clientEvents"])
        .astype(int)
    )


def agg_facility_programs(facility_id, program_list, match, prefix):
    """
    Aggregate the current bed occupancy data for a list of programs,
    filtering by program name.

    Parameters
    ==========

    facility_id: int
        The facility id.

    program_list: list
        A list of programs of the shape returned by the GetHelp
        facility-programs endpoint.

    match: str
        A string which is tested for inclusion in a program name
        to decide whether to include a program in the statistics.

    prefix:
        A string to prefix series labels with.

    Returns
    =======
    A pandas.Series with the aggregated statistics for the matching facility programs.
    """
    # A sentinel timestamp which is used to determine whether
    # any programs actually matched.
    sentinel = pandas.Timestamp("2020-01-01T00:00:00Z")
    last_updated = functools.reduce(
        lambda x, y: (
            max(x, pandas.Timestamp(y["lastUpdated"]))
            if match in y["name"].lower()
            else x
        ),
        program_list,
        sentinel,
    )
    if last_updated == sentinel:
        # No programs matched, return early
        return None

    occupied = functools.reduce(
        lambda x, y: x
        + (y["bedsOccupied"] + y["bedsPending"] if match in y["name"].lower() else 0),
        program_list,
        0,
    )
    total = functools.reduce(
        lambda x, y: x + (y["bedsTotal"] if match in y["name"].lower() else 0),
        program_list,
        0,
    )
    available = total - occupied
    client_stats = functools.reduce(
        lambda x, y: x.add(
            get_program_client_stats(facility_id, y["id"]), fill_value=0,
        )
        if match in y["name"].lower()
        else x,
        program_list,
        pandas.Series(),
    )
    return pandas.Series(
        {
            prefix + "occupied": occupied,
            prefix + "available": available,
            prefix + "last_updated": last_updated,
        }
    ).append(client_stats.rename(lambda x: prefix + x))


def get_facility_program_status(facility_id):
    """
    Get the most recent status for a facility, broken
    up into shelter beds, trailers, and safe parking.

    Parameters
    ==========

    facility_id: int
        The facility ID.

    Returns
    =======
    A pandas.Series with program statistics for shelter beds, safe
    parking, and trailer beds.
    """
    TOKEN = Variable.get("GETHELP_OAUTH_PASSWORD")
    res = make_get_help_request(f"facilities/{facility_id}/facility-programs", TOKEN)
    shelter_beds = agg_facility_programs(
        facility_id, res, "shelter bed", "shelter_beds_"
    )
    isolation = agg_facility_programs(facility_id, res, "isolation", "isolation_")
    trailers = agg_facility_programs(facility_id, res, "trailer", "trailers_")
    safe_parking = agg_facility_programs(facility_id, res, "parking", "safe_parking_")
    return pandas.concat([shelter_beds, isolation, trailers, safe_parking])


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

    history = pandas.DataFrame()
    if not len(programs):
        return history

    # Get the history stats for the shelter bed programs
    for _, program in programs.iterrows():
        program_id = program["id"]
        res = make_get_help_request(
            f"facilities/{facility_id}/facility-programs/{program_id}/statistics",
            TOKEN,
            params={"startDate": str(start_date), "endDate": str(end_date)},
        )
        program_history = pandas.io.json.json_normalize(res)
        # Add ID column so we can filter by them later
        program_history = program_history.assign(program_id=program_id)
        history = history.append(program_history)

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
    print(f"Loading timeseries for {facility['name']}")
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
        district=facility["district"],
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
    upload_to_esri(facilities, FACILITIES_ID, "/tmp/gethelp-facilities-v5.csv")
    timeseries = assemble_get_help_timeseries()
    upload_to_esri(timeseries, TIMESERIES_ID, "/tmp/gethelp-timeseries-v2.csv")

    # Compute a number of open and reporting shelter beds
    active_facilities = facilities[facilities.status != 0]
    stats = {
        "n_shelters": len(facilities),
        "n_shelters_status_known": len(active_facilities),
        "n_shelters_with_available_beds": len(
            active_facilities[active_facilities.status == 1]
        ),
        "n_available_beds": active_facilities.availableBeds.sum(),
        "n_occupied_beds": active_facilities.totalBeds.sum()
        - active_facilities.availableBeds.sum(),
    }
    stats_df = pandas.DataFrame.from_dict(
        stats, orient="index", columns=["Count"]
    ).transpose()
    # TODO: Write an assert to make sure all rows are in resultant GDF
    upload_to_esri(stats_df, STATS_ID, "/tmp/gethelp-stats.csv")

    # push the tables into kwargs for email
    kwargs["ti"].xcom_push(key="facilities", value=active_facilities)
    kwargs["ti"].xcom_push(key="stats_df", value=stats_df)


def integrify(x):
    return str(int(x)) if not pandas.isna(x) else "Error"


def format_program_client_stats(row, prefix):
    """
    Given a program in the facility DF (specified by string prefix),
    format the client stats (gender, pets, ADA, EMS calls/visits).

    Parameters:
    ===========

    row: pandas.Series
        The row of the df to format
    prefix: str
        The prefix for all the stats entries (e.g., 'trailers_', 'isolation_', etc)

    Returns
    =======

    An HTML string of the formatted client stats.
    """
    men = integrify(row[prefix + "MALE"] + row[prefix + "TRANSGENDER_F_TO_M"])
    women = integrify(row[prefix + "FEMALE"] + row[prefix + "TRANSGENDER_M_TO_F"])
    nonbinary = integrify(
        row[prefix + "DECLINED"] + row[prefix + "OTHER"] + row[prefix + "UNDEFINED"]
    )
    pets = integrify(row[prefix + "totalPets"])
    ada = integrify(row[prefix + "totalAda"])
    ems_calls = integrify(row[prefix + "EMS_CALL"])
    ems_visits = integrify(row[prefix + "EMS_VISIT"])
    return f"""
        <p style="margin-top:2px; margin-bottom: 2px; margin-left: 16px">
            Women: {women}
        </p>
        <p style="margin-top:2px; margin-bottom: 2px; margin-left: 16px">
            Men: {men}
        </p>
        <p style="margin-top:2px; margin-bottom: 2px; margin-left: 16px">
            Nonbinary/other/declined: {nonbinary}
        </p>
        <p style="margin-top:2px; margin-bottom: 2px; margin-left: 16px">
            Pets: {pets}
        </p>
        <p style="margin-top:2px; margin-bottom: 2px; margin-left: 16px">
            Clients with ADA needs: {ada}
        </p>
        <p style="margin-top:2px; margin-bottom: 2px; margin-left: 16px">
            EMS calls (last 24 hours): {ems_calls}
        </p>
        <p style="margin-top:2px; margin-bottom: 2px; margin-left: 16px">
            EMS visits (last 24 hours): {ems_visits}
        </p>
    """


def format_table(row):
    """
    returns a nicely formatted HTML
    for each Shelter row
    """
    shelter_name = row["name"]
    district = row["district"]

    # a sentinel timestamp to use as comparisons against updates,
    # this should be older than any of the program updateds, and
    # can be used to determine if it has never been updated.
    old_ts = pandas.Timestamp("2020-01-01T00:00:00Z")

    # Shelter stats
    shelter_occ = integrify(row["shelter_beds_occupied"] or 0)
    shelter_avail = integrify(row["shelter_beds_available"] or 0)
    shelter_updated = (
        row["shelter_beds_last_updated"]
        if not pandas.isna(row["shelter_beds_last_updated"])
        else old_ts
    )

    # Isolation stats
    isolation_occ = integrify(row["isolation_occupied"] or 0)
    isolation_updated = (
        row["isolation_last_updated"]
        if not pandas.isna(row["isolation_last_updated"])
        else old_ts
    )

    # Trailer stats
    trailer_occ = integrify(row["trailers_occupied"] or 0)
    trailer_avail = integrify(row["trailers_available"] or 0)
    trailer_updated = (
        row["trailers_last_updated"]
        if not pandas.isna(row["trailers_last_updated"])
        else old_ts
    )

    # Safe parking stats
    safe_parking_occ = integrify(row["safe_parking_totalClients"] or 0)
    safe_parking_updated = (
        row["safe_parking_last_updated"]
        if not pandas.isna(row["safe_parking_last_updated"])
        else old_ts
    )

    # Determine the most recent update across all programs
    last_update = max(
        shelter_updated, isolation_updated, safe_parking_updated, trailer_updated
    )
    last_update = (
        last_update.tz_convert("US/Pacific").strftime("%m-%d-%Y %I:%M%p")
        if last_update != old_ts
        else "Never"
    )

    # Create the email body.
    entry = f"""<b>{shelter_name}</b><br>
    <i>Council District {district}</i><br>
    <i>Last change at site: {last_update}</i><br><br>
    """

    if shelter_updated != old_ts:
        entry = (
            entry
            + f"""
            <p style="margin-top:2px; margin-bottom: 2px">
                Available Shelter Beds: {shelter_avail}
            </p>
            <p style="margin-top:2px; margin-bottom: 2px">
                Occupied Shelter Beds: {shelter_occ}
            </p>
            {format_program_client_stats(row, "shelter_beds_")}
            <br>
            """
        )
    if isolation_updated != old_ts and isolation_occ != "0":
        entry = (
            entry
            + f"""
            <p style="margin-top:2px; margin-bottom: 2px">
                Clients in isolation: {isolation_occ}
            </p>
            {format_program_client_stats(row, "isolation_")}
           <br>
           """
        )
    if trailer_updated != old_ts:
        entry = (
            entry
            + f"""
            <p style="margin-top:2px; margin-bottom: 2px">
                Available Trailers: {trailer_avail}
            </p>
            <p style="margin-top:2px; margin-bottom: 2px">
                Occupied Trailers: {trailer_occ}
            </p>
            {format_program_client_stats(row, "trailers_")}
            <br>
            """
        )
    if safe_parking_updated != old_ts and safe_parking_occ != "0":
        entry = (
            entry
            + f"""
            <p style="margin-top:2px; margin-bottom: 2px">
                Occupied Safe Parking: {safe_parking_occ}
            </p>
            {format_program_client_stats(row, "safe_parking_")}
           <br>
           """
        )

    entry = entry + "<br>"

    return entry.strip()


def email_function(**kwargs):
    """
    Sends a hourly email with the latest updates from each shelter
    Formatted for use
    """
    airflow_timestamp = pandas.to_datetime(kwargs["ts"]).tz_convert("US/Pacific")
    # The end of the 45 minute schedule interval corresponds to the top
    # of the hour, so only email during that run.
    if airflow_timestamp.minute != 45:
        return True

    facilities = kwargs["ti"].xcom_pull(key="facilities", task_ids="load_get_help_data")
    stats_df = kwargs["ti"].xcom_pull(key="stats_df", task_ids="load_get_help_data")
    exec_time = (
        pandas.Timestamp.now(tz="US/Pacific")
        .replace(minute=0)
        .strftime("%m-%d-%Y %I:%M%p")
    )
    # Sort by council district and facility name.
    facilities = facilities.sort_values(["district", "name"])
    tbl = numpy.array2string(
        facilities.apply(format_table, axis=1).str.replace("\n", "").values
    )
    tbl = tbl.replace("""'\n '""", "").lstrip(""" [' """).rstrip(""" '] """)

    # Get some top-level summary statistics
    summary = facilities.sum()
    shelter_occ = integrify(summary["shelter_beds_occupied"] or 0)
    shelter_avail = integrify(summary["shelter_beds_available"] or 0)
    trailer_occ = integrify(summary["trailers_occupied"] or 0)
    trailer_avail = integrify(summary["trailers_available"] or 0)
    safe_parking_occ = integrify(summary["safe_parking_totalClients"] or 0)

    email_body = f"""
    <h3><b>PLEASE DO NOT REPLY TO THIS EMAIL </b></h3>
    <p>Questions should be sent directly to rap.dutyofficer@lacity.org</p>
    <br>
    Shelter Report for {exec_time}.
    <br>

    The Current Number of Reporting Shelters is
    {integrify(stats_df['n_shelters_status_known'][0])}.
    <br><br>

    <h3><b>System Summary</b></h3>
    <p style="margin-top:2px; margin-bottom: 2px">
        Available Shelter Beds: {shelter_avail}
    </p>
    <p style="margin-top:2px; margin-bottom: 2px">
        Occupied Shelter Beds: {shelter_occ}
    </p>
    {format_program_client_stats(summary, "shelter_beds_")}
    <br>

    <p style="margin-top:2px; margin-bottom: 2px">
        Available Trailers: {trailer_avail}
    </p>
    <p style="margin-top:2px; margin-bottom: 2px">
        Occupied Trailers: {trailer_occ}
    </p>
    {format_program_client_stats(summary, "trailers_")}
    <br>

    <p style="margin-top:2px; margin-bottom: 2px">
        Occupied Safe Parking: {safe_parking_occ}
    </p>
    {format_program_client_stats(summary, "safe_parking_")}
    <br><br>

    <h3><b>Individual Facilities</b></h3>

    {tbl}

    <br>

    """

    if airflow_timestamp.hour + 1 in [8, 12, 15, 17, 20]:
        email_list = ["rap-shelter-updates@lacity.org"]
    else:
        email_list = [
            "itadata@lacity.org",
            "jimmy.kim@lacity.org",
            "rap.dutyofficer@lacity.org",
        ]

    send_email(
        to=email_list,
        subject=f"""Shelter Stats for {exec_time}""",
        html_content=email_body,
    )
    return True


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.datetime(2020, 4, 11),
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
    provide_context=True,
    python_callable=load_get_help_data,
    op_kwargs={},
    dag=dag,
)

t2 = PythonOperator(
    task_id="send_shelter_email",
    provide_context=True,
    python_callable=email_function,
    dag=dag,
)

t1 >> t2
