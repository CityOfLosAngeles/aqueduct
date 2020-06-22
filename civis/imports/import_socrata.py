"""
An Import Socrata Template for Deployment on Civis Platform
Author: @sherryshenker
"""

from sodapy import Socrata
import pandas as pd
import logging
import os
from datetime import datetime
import civis

from socrata_helpers import (
    _store_and_attach_dataset_csv,
    write_and_attach_jsonvalue,
    _store_and_attach_metadata,
    get_dates,
    write_csv,
    _read_paginated
)

LOG = logging.getLogger(__name__)


def main(
    dataset_id: str,
    civis_table_name: str,
    civis_database: str,
    socrata_username: str,
    socrata_password: str,
    grant_group: str,
    action_existing_table_rows: str = "drop"
):
    """
    Read in dataset from Socrata and write output to Platform
    Parameters
    --------
    dataset_id: str
        Socrata dataset identifier
    civis_table_name: str, optional
        destination table in Platform (schema.table)
    civis_database: str, optional
        destination database in Platform
    socrata_username: str, optional
        username for socrata account, required for private data sets
    socrata_password: str, optional
        password for socrata account, required for private data sets
    date_column: str, optional
        date column to be used for SoQL filtering of dataset
    splits: int, optional
        number of splits that should be made on dataset
    action_existing_table_rows: str, optional
        options to pass to dataframe_to_civis command
    Outputs
    ------
    Adds data as file output
    and, if table_name and database are specified, writes data to Platform
    """

    socrata_client = Socrata(
        "data.lacity.org",
        None,
        username=socrata_username,
        password=socrata_password
    )

    socrata_client.timeout = 50

    raw_metadata = socrata_client.get_metadata(dataset_id)

    consolidated_csv_path = _read_paginated(
        client=socrata_client, dataset_id=dataset_id
    )
    # this will read in socrata data in chunks (using offset and page_limit), and
    # append all to one csv and output path here

    civis_client = civis.APIClient()

    dataset = pd.read_csv(consolidated_csv_path, nrows=5)
    # only putting a couple rows in memory for logging
    if dataset.empty:
        msg = f"No rows returned for dataset {dataset_id}."
        LOG.warning(msg)
        write_and_attach_jsonvalue(json_value=msg, name="Error", client=civis_client)
    else:
        data_file_name = (
            f"{dataset_id}_extract_{datetime.now().strftime('%Y-%m-%d')}.csv"
        )
        uploaded_file_id = _store_and_attach_dataset_csv(
            client=civis_client, csv_path=consolidated_csv_path, filename=data_file_name
        )
        LOG.info(f"add the {uploaded_file_id}")

        if civis_table_name:
            # Optionally start table upload
            LOG.info(f"Storing data in table {civis_table_name} on database {civis_database}")
            print("writing table")
            ##takes in file id and writes to table
            table_upload = civis.io.civis_file_to_table(
                file_id=uploaded_file_id,
                database=civis_database,
                table=civis_table_name,
                existing_table_rows=action_existing_table_rows,
                headers=True
            ).result()
            LOG.info(f"using {table_upload}")

    # Parse raw_metadata to extract useful fields and attach both raw and
    # cleaned metadata as script outputs
    metadata_file_name = (
        f"{dataset_id}_metadata_{datetime.now().strftime('%Y-%m-%d')}.json"
    )

    upload_metadata_paths = {
        "Proposed access level": "metadata.custom_fields.Proposed Access Level.Proposed Access Level",
        "Description": "description",
        "Data updated at": "rowsUpdatedAt",
        "Data provided by": "tableAuthor.screenName"
    }

    _, clean_metadata = _store_and_attach_metadata(
        client=civis_client,
        metadata=raw_metadata,
        metadata_paths=upload_metadata_paths,
        filename=metadata_file_name,
    )

    if civis_table_name:
        sql = f'COMMENT ON TABLE {civis_table_name} IS \'{clean_metadata["Description"]}\''
        civis.io.query_civis(
            sql, database=civis_database, polling_interval=2, client=civis_client
        ).result()

    if grant_group:
        sql = f'GRANT ALL ON {civis_table_name} TO GROUP {grant_group}'
        civis.io.query_civis(
            sql, database=civis_database, polling_interval=2, client=civis_client
        ).result()

if __name__ == "__main__":
    DATASET_ID = os.environ["dataset_id"]
    EXISTING_TABLE_ROWS = "drop"
    if "table_name" in list(os.environ.keys()) and "database" in list(
        os.environ.keys()
    ):
        TABLE_NAME = os.environ["table_name"]
        CIVIS_DATABASE = os.environ["database"]
        GRANT_GROUP = os.environ["group"]
    else:
        TABLE_NAME = None
        CIVIS_DATABASE = None
        GRANT_GROUP = None
    if "socrata_username" in list(os.environ.keys()):
        SOCRATA_USERNAME = os.environ["socrata_username"]
        SOCRATA_PASSWORD = os.environ["socrata_password"]
    else:
        SOCRATA_USERNAME = None
        SOCRATA_PASSWORD = None
    main(DATASET_ID, TABLE_NAME, CIVIS_DATABASE, SOCRATA_USERNAME, SOCRATA_PASSWORD, GRANT_GROUP)
