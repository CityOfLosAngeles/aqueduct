"""
Helpers functions for the socrata import
"""
import pandas as pd
import os
import civis
from typing import Optional, Tuple
from datetime import datetime
from civis import APIClient
import logging
import json
from functools import reduce
import numpy as np

from civis.io import dataframe_to_file, file_to_civis, civis_file_to_table, query_civis

LOG = logging.getLogger(__name__)


def _parse_metadata(metadata: dict, paths: dict):
    out = {}
    for name, path in paths.items():
        out[name] = reduce(
            lambda d, key: d.get(key, None) if isinstance(d, dict) else None,
            path.split("."),
            metadata
        )
    return out


def _store_and_attach_metadata(
    client: APIClient, metadata: dict, metadata_paths: dict, filename: str,
) -> Tuple[int, dict]:
    """
    Given an APIClient object, metadata read from DDL, a collection of keys
    and paths within the DDL metadata, and a filename, this function:
        (1) writes the cleaned metadata fields to a JSONValue object,
        (2) writes the raw metadata fields to a file object,
        (3) attaches both to the current script as outputs, and
        (4) returns the file_id of the raw metadata and the cleaned metadata
        as a dictionary.
    Parameters
    ---------
    client: APIClient
        An instance of civis.APIClient.
    metadata: dict
        The raw metadata read from DDL.
    metadata_paths: dict
        A dictionary of the paths used to clean the metadata read from DDL.
        This should be the value of ddl_metadata_paths in configs.constants.
    filename: str
        The name of the file to which raw metadata should be written.
    Returns
    -------
    Tuple[int, dict]:
        file_id (int) of the raw metadata stored in S3, and
        cleaned_metadata (dict)
    Side Effects
    ------------
    - Stores object passed to metadata argument as a .json file in S3
    - Attaches this .json file as a script output
    - Stores cleaned metadata object as a JSONValues object
    - Attaches this JSONValues object as a script output
    """

    with open(filename, "w") as f:
        json.dump(metadata, f)
    file_id = file_to_civis(buf=filename, name=filename, expires_at=None,)
    client.scripts.post_containers_runs_outputs(
        id=os.environ["CIVIS_JOB_ID"],
        run_id=os.environ["CIVIS_RUN_ID"],
        object_type="File",
        object_id=file_id
    )

    cleaned_metadata = _parse_metadata(metadata=metadata, paths=metadata_paths)
    for key, value in cleaned_metadata.items():
        if key.lower().endswith("updated at"):
            value = datetime.fromtimestamp(value).strftime("%Y-%m-%d %H:%M")
        write_and_attach_jsonvalue(json_value=value, name=key, client=client)
    return file_id, cleaned_metadata


def write_and_attach_jsonvalue(
    json_value: str, name: str, client: APIClient = None,
) -> None:
    json_obj = client.json_values.post(json.dumps(json_value), name=name)
    client.scripts.post_containers_runs_outputs(
        id=os.environ["CIVIS_JOB_ID"],
        run_id=os.environ["CIVIS_RUN_ID"],
        object_type="JSONValue",
        object_id=json_obj.id
    )


def _store_and_attach_dataset_csv(
    client: civis.APIClient, csv_path: str, filename: str
) -> int:
    """
    Given an APIClient object, a csv path, and a filename, write the csv
    to a file, attach the file as a script output, and return the file_id.
    Parameters
    ---------
    client: APIClient
        An instance of civis.APIClient.
    csv_path: str
        A string containg the path of CSV
    filename: str
        The name of the file to which data should be written.
    Returns
    -------
    int: file_id of the file stored in S3
    Side Effects
    ------------
    - Stores object passed to df argument as a .csv file in S3
    - Attaches this .csv file as a script output
    """
    file_id = civis.io.file_to_civis(csv_path, name=filename, expires_at=None)
    client.scripts.post_containers_runs_outputs(
        id=os.environ["CIVIS_JOB_ID"],
        run_id=os.environ["CIVIS_RUN_ID"],
        object_type="File",
        object_id=file_id
    )
    return file_id


def _store_and_attach_metadata(
    client: civis.APIClient, metadata: dict, metadata_paths: dict, filename: str
) -> Tuple[int, dict]:
    """
    Given an APIClient object, metadata read from DDL, a collection of keys
    and paths within the DDL metadata, and a filename, this function:
        (1) writes the cleaned metadata fields to a JSONValue object,
        (2) writes the raw metadata fields to a file object,
        (3) attaches both to the current script as outputs, and
        (4) returns the file_id of the raw metadata and the cleaned metadata
        as a dictionary.
    Parameters
    ---------
    client: APIClient
        An instance of civis.APIClient.
    metadata: dict
        The raw metadata read from DDL.
    metadata_paths: dict
        A dictionary of the paths used to clean the metadata read from DDL.
        This should be the value of ddl_metadata_paths in configs.constants.
    filename: str
        The name of the file to which raw metadata should be written.
    Returns
    -------
    Tuple[int, dict]:
        file_id (int) of the raw metadata stored in S3, and
        cleaned_metadata (dict)
    Side Effects
    ------------
    - Stores object passed to metadata argument as a .json file in S3
    - Attaches this .json file as a script output
    - Stores cleaned metadata object as a JSONValues object
    - Attaches this JSONValues object as a script output
    """

    with open(filename, "w") as f:
        json.dump(metadata, f)
    file_id = file_to_civis(buf=filename, name=filename, expires_at=None,)
    client.scripts.post_containers_runs_outputs(
        id=os.environ["CIVIS_JOB_ID"],
        run_id=os.environ["CIVIS_RUN_ID"],
        object_type="File",
        object_id=file_id
    )

    cleaned_metadata = _parse_metadata(metadata=metadata, paths=metadata_paths)
    for key, value in cleaned_metadata.items():
        if key.lower().endswith("updated at"):
            value = datetime.fromtimestamp(value).strftime("%Y-%m-%d %H:%M")
        write_and_attach_jsonvalue(json_value=value, name=key, client=client)
    return file_id, cleaned_metadata


def _read_paginated(
    client, dataset_id: int, page_limit: int = 90000, size_limit: int = None
):
    """
    Uses socrata API get call to pull i.
    Pulls in Socrata data in a while loop using pandas dataframes (in 80000 row
    chunks) that get written to .csv(s). .csv(s) and then appened together and
    a path to final csv is outputted.
    Inputs
    _____________
    dataset_id: str
        Socrata dataset identifier
    page_limit: int
        number of records that can be pulled in chunk
    date_column: 'str'
    Outputs
    _____________
    path of appened csv
    """
    df = pd.DataFrame()
    offset = 0
    paths = []
    while True:
        LOG.debug(f"Downloading data at offset {offset} of {dataset_id}")
        results = client.get(
            dataset_id,
            limit=page_limit,
            content_type="csv",
            exclude_system_fields=False,
            offset=offset
        )

        if not results[1:]:
            LOG.debug(f"All available results read from dataset {dataset_id}.")
            break

        path = "df" + str(offset) + ".csv"
        paths = np.append(path, paths)
        df = pd.DataFrame(results[1:], columns=results[0])

        df.to_csv(path, header=False, index=False)

        df.columns = map(
            str.lower, df.columns
        )  ##converting headers to lower and pulling
        headers = df.columns.str.cat(sep=",")

        if len(results) - 1 < page_limit:
            LOG.debug(f"All available results read from dataset {dataset_id}.")
            break

        if size_limit and offset >= size_limit:
            LOG.info(
                f"Reached requested row count limit, {size_limit}, for dataset "
                f"{dataset_id}."
            )
            LOG.debug(
                f"Pulled {offset} rows, in increments of {page_limit}, up to "
                f"specified limit of {size_limit} rows."
            )
            break

        offset += page_limit

    csv_out = write_csv(paths, headers)

    return csv_out


def write_csv(paths, headers):
    csv_out = "consolidated.csv"
    csv_merge = open(csv_out, "w")
    csv_merge.write(headers)
    csv_merge.write("\n")

    for path in paths:
        with open(path, "r") as data:
            csv = data.read()
        csv_merge.write(csv)
    return csv_out


def _read_paginated_chunks_bydate(
    client,
    dataset_id,
    page_limit: int = 500000,
    size_limit: int = None,
    date_column: str = "createddate",
    splits: int = 15
):

    """
    Uses socrata get calls to query data in evenly spaced chunks (by date)
    writes to pd.df then to csv. All csvs then appended into one large csv.
    only one chunk in memory at time, as csv is on disk.
    Inputs
    _____________
    dataset_id: str
        Socrata dataset identifier
    page_limit: int
        number of records that can be pulled in chunk
    date_column: 'str'
        specifies the date column that we want to split on
    splits: int
        number of even splits that are made on date range.
    Outputs
    _____________
    path of appened csv
    """
    print(dataset_id)
    offset = 0
    paths = []
    min_date, max_date, date_splits = get_dates(
        client, dataset_id, date_column="createddate", splits=splits
    )

    ### here we bring in the data, use pandas to put into dataframe and then
    ### strip header when writing to CSV
    ### We also build out paths for the csvs to be stored in and store the path names
    for i in np.arange(len(date_splits)):
        if i == len(date_splits) - 1:
            break

        where = (
            date_column
            + " <= '"
            + date_splits[i]
            + "' AND "
            + date_column
            + " > '"
            + date_splits[i + 1]
            + "'"
        )

        df = pd.DataFrame()

        results = socrata_client.get(
            dataset_id,
            limit=page_limit,
            content_type="csv",
            exclude_system_fields=False,
            offset=offset,
            where=where
        )

        df = pd.concat([df, pd.DataFrame(results[1:], columns=results[0])])
        print(where)
        print(len(df))

        path = "df" + str(i) + ".csv"
        paths = np.append(path, paths)

        df.to_csv(path, header=False, index=False)

        df.columns = map(
            str.lower, df.columns
        )  ##converting headers to lower and pulling
        headers = df.columns.str.cat(
            sep=","
        )  ##out headers as string to use when building big csv

    csv_out = write_csv(paths, headers)

    return csv_out


def get_dates(
    client, dataset_id: str, date_column: str = "createddate", splits: int = 70
):
    """
    Uses socrata get calls to query for most recent and oldest dates
    within the dataset, outputs those values, and creates a date range
    based on number of splits. Can be used for date parsing.
    Inputs
    _____________
    dataset_id: str
        Socrata dataset identifier
    splits: int
        number of even splits that are made on date range.
    date_column: 'str'
        specifies the date column that we want to split on
    Outputs
    _____________
    min_date: str
        Oldest date in dataset
    max_date: str
        most recent date in dataset
    date_splits: array
        array of n dates set by splits param.
    """
    min_date_query = (
        "SELECT " + date_column + " Order by " + date_column + " ASC limit 1"
    )

    min_date = client.get(dataset_id, content_type="csv", query=min_date_query)

    max_date_query = (
        "SELECT " + date_column + " Order by " + date_column + " DESC limit 1"
    )

    max_date = client.get(dataset_id, content_type="csv", query=max_date_query)

    max_date = pd.to_datetime(max_date[1][0])
    min_date = pd.to_datetime(min_date[1][0])

    date_splits_raw = pd.date_range(start=min_date, end=max_date, periods=int(splits))

    date_splits = np.array([])

    for date in date_splits_raw:
        date = str(date)[:10]
        date_splits = np.append(date, date_splits)

    return min_date, max_date, date_splits
