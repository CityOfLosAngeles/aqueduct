"""
Scrape Los Angeles Metro ridership data
"""
import os

import bs4
import pandas as pd
import requests

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from datetime import datetime, timedelta

# The URL for the ridership form
RIDERSHIP_URL = "http://isotp.metro.net/MetroRidership/IndexSys.aspx"

# Parameters needed to validate the request
ASPX_PARAMETERS = ["__VIEWSTATE", "__EVENTVALIDATION"]


def get_form_data():
    """
    Make an inital fetch of the form so we can scrape the options
    as well as the parameters needed to validate our requests.
    """
    # Fetch the page and parse it
    r = requests.get(RIDERSHIP_URL)
    r.raise_for_status()
    soup = bs4.BeautifulSoup(r.text, features="html.parser")

    # Get validation info
    aspx_data = {
        param: soup.find(attrs={"id": param}).attrs.get("value", "")
        for param in ASPX_PARAMETERS
    }

    # Get all of the metro lines
    line_options = soup.find(attrs={"id": "ContentPlaceHolder1_lbLines"}).select(
        "option"
    )
    lines = [
        option.attrs["value"]
        for option in line_options
        if option.attrs["value"] != "All"
    ]

    # Get the available years
    year_options = soup.find(attrs={"id": "ContentPlaceHolder1_ddlYear"}).select(
        "option"
    )
    years = [option.attrs["value"] for option in year_options]
    return lines, years, aspx_data


def submit_form(year, period, line, aspx_data):
    """
    Submit a form to the Metro ridership site requesting data for a line.

    Parameters
    ----------
    year: int
        The year for which to fetch the data.
    period: str or int
        The time period in which to fetch the data. Typically you will want
        an integer month, though other values like quarters ("Q1") may work.
    line: str or int
        The Metro line number
    aspx_data: dict
        The metadata needed for forming a correct form submission.

    Returns
    -------
    An HTML string of the resposnse.
    """
    form_data = {
        "ctl00$ContentPlaceHolder1$rbFYCY": "CY",
        "ctl00$ContentPlaceHolder1$ddlYear": str(year),
        "ctl00$ContentPlaceHolder1$ddlPeriod": str(period),
        "ctl00$ContentPlaceHolder1$btnSubmit": "Submit",
        "ctl00$ContentPlaceHolder1$lbLines": str(line),
        **aspx_data,
    }
    r = requests.post(RIDERSHIP_URL, data=form_data)
    r.raise_for_status()
    if r.text.find("Data not available yet") != -1:
        raise ValueError(f"Data not available for {year}, {period}, {line}")

    return r.text


def parse_response(html):
    """
    Parse an HTML response from the ridership form into a dataframe.

    Parameters
    ----------
    html: str
        The HTML webpage from the ridership site.

    Returns
    -------
    A dataframe from the parsed HTML table.
    """
    tables = pd.read_html(
        html,
        flavor="bs4",
        attrs={"id": "ContentPlaceHolder1_ASPxRoundPanel2_gvRidership"},
    )
    if len(tables) == 0:
        raise ValueError("No table found")
    df = tables[0]
    # Filter out the "Total" row
    df = df[df["Day Type"] != "Total"]
    return df


def get_ridership_data(year, period, line, aspx_data):
    """
    Get ridership for a given year, time period, and line.

    Parameters
    ----------
    year: int
        The year for which to fetch the data.
    period: str or int
        The time period in which to fetch the data. Typically you will want
        an integer month, though other values like quarters ("Q1") may work.
    line: str or int
        The Metro line number
    aspx_data: dict
        The metadata needed for forming a correct form submission.

    Returns
    -------
    A dataframe with ridership data for the line/period/year.
    """
    html = submit_form(year, period, line, aspx_data)
    df = parse_response(html)

    df = df.assign(year=year, month=period, line=line)
    return df


def get_all_ridership_data(verbosity=0):
    """
    Fetch all ridership data from the web form.
    """
    lines, years, aspx_data = get_form_data()
    months = [str(i) for i in range(1, 13)]
    ridership = pd.DataFrame()
    # Get the current timestamp so we don't try to fetch from the future.
    now = pd.Timestamp.now()

    for year in years:
        # Don't try to fetch from years in the future.
        if int(year) > now.year:
            continue
        if verbosity > 0:
            print(f"Fetching data for {year}")
        for month in months:
            # Don't try to fetch from months in the future
            if int(year) == now.year and int(month) >= now.month:
                continue
            if verbosity > 1:
                print(f"Fetching data for month {month}")
            for line in lines:
                try:
                    df = get_ridership_data(year, month, line, aspx_data)
                    ridership = ridership.append(df)
                    if verbosity > 2:
                        print(f"Fetched data for line {line}")
                except:
                    if verbosity > 2:
                        print(f"Failed to get data for line {line}")
    return ridership


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 10, 30),
    "email": ["ian.rose@lacity.org"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(days=1),
}

dag = DAG("metro-ridership", default_args=default_args, schedule_interval="@monthly")


def scrape_ridership_data(**kwargs):
    """
    The actual python callable that Airflow schedules.
    """
    name = kwargs.get("name", "metro-ridership.parquet")
    bucket = kwargs.get("bucket")
    ridership = get_all_ridership_data(3)
    if bucket:
        ridership.to_parquet(name)
        s3 = S3Hook('s3_conn')
        s3.load_file(name, name, bucket)
        os.remove(name)


t1 = PythonOperator(
    task_id="scrape-ridership-data",
    python_callable=scrape_ridership_data,
    op_kwargs={
        "name": "metro-ridership.parquet",
        "bucket": "tmf-data"
    },
    dag=dag,
)
