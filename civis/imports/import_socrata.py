"""
An Import Socrata Template for Deployment on Civis Platform
Author: @sherryshenker, @snassef, @akoebs
"""

from sodapy import Socrata
import logging
import os
from datetime import datetime
import civis

from socrata_helpers import (
    _store_and_attach_dataset_csv,
    write_and_attach_jsonvalue,
    _store_and_attach_metadata,
    create_col_type_dict,
    _read_paginated,
    select_sql_map,
    results_to_df,
)

LOG = logging.getLogger(__name__)


def main(
    socrata_client_url: str,
    dataset_id: str,
    civis_table_name: str,
    civis_database: str,
    database_type: str,
    socrata_username: str,
    socrata_password: str,
    grant_group: str,
    varchar_len: str = None,
    action_existing_table_rows: str = "drop",
):
    """
    Read in dataset from Socrata and write output to Platform

    Parameters
    --------
    socrata_client_url: str
        url of socrata portal being referenced
    dataset_id: str
        Socrata dataset identifier
    civis_table_name: str
        destination table in Platform (schema.table)
    civis_database: str
        destination database in Platform
    database_type: str
        type of destination database
    socrata_username: str, optional
        username for socrata account, required for private data sets
    socrata_password: str, optional
        password for socrata account, required for private data sets
    grant_group: str
        string of group(s) that are passed to civis API to be granted select
        table access
    varchar_len: str
        sets the varchar length when datatypes are passed to civis API, 256 is
        defualt
    action_existing_table_rows: str, optional
        options to pass to dataframe_to_civis command

    Outputs
    ------
    Adds data as file output and, if table_name and database are specified,
    writes data to Platform
    """

    socrata_client = Socrata(
        socrata_client_url, None, username=socrata_username, password=socrata_password
    )
    # define socrata cleint

    civis_client = civis.APIClient()
    # define civis cleint

    socrata_client.timeout = 50

    sample_data = socrata_client.get(
        dataset_id, limit=5, content_type="csv", exclude_system_fields=False, offset=0
    )
    # collects sample data from dataset

    sample_data_df = results_to_df(sample_data)
    # writes sample data to dataframe

    if sample_data_df.empty:
        msg = f"No rows returned for dataset {dataset_id}."
        LOG.warning(msg)
        write_and_attach_jsonvalue(json_value=msg, name="Error", client=civis_client)
        os._exit(1)
    # provides exit if no rows avalible in dataset

    raw_metadata = socrata_client.get_metadata(dataset_id)
    # calls for raw metadata

    sql_type = select_sql_map(database_type, varchar_len)
    # defines apropriate sql types for datatype mapping depending on
    # specifications

    (
        civis_table_columns,
        point_columns,
        pandas_column_order,
        extra_columns,
    ) = create_col_type_dict(raw_metadata, sample_data_df, sql_type)
    # creates civis specific array of dicts that maps column name to
    # datatype using socrata metadata as guidence. Also, provides point
    # columns that are used to clean point column formatting during import.
    # And, provides array of columns that corresponds to order of the mapping
    # dict (civis_file_to_table is sensitive to order.

    print("Columns present in Metadata but not in data:", extra_columns)

    consolidated_csv_path = _read_paginated(
        client=socrata_client,
        dataset_id=dataset_id,
        point_columns=point_columns,
        column_order=pandas_column_order,
    )
    # reads in socrata data in chunks (using offset and page_limit), and
    # appenda all to one csv and outputs path here

    data_file_name = f"{dataset_id}_extract_{datetime.now().strftime('%Y-%m-%d')}.csv"
    uploaded_file_id = _store_and_attach_dataset_csv(
        client=civis_client, csv_path=consolidated_csv_path, filename=data_file_name
    )
    print("file_id:", uploaded_file_id)
    LOG.info(f"add the {uploaded_file_id}")

    LOG.info(f"Storing data in table {civis_table_name} on database {civis_database}")

    table_upload = civis.io.civis_file_to_table(
        file_id=uploaded_file_id,
        database=civis_database,
        table=civis_table_name,
        table_columns=civis_table_columns,
        existing_table_rows=action_existing_table_rows,
        headers=True,
    ).result()
    LOG.info(f"using {table_upload}")
    # takes in file id and writes to table

    metadata_file_name = (
        f"{dataset_id}_metadata_{datetime.now().strftime('%Y-%m-%d')}.json"
    )
    # parse raw_metadata to extract useful fields and attach both raw and
    # cleaned metadata as script outputs

    upload_metadata_paths = {
        "Description": "description",
        "Data updated at": "rowsUpdatedAt",
        "Data provided by": "tableAuthor.screenName",
    }

    _, clean_metadata = _store_and_attach_metadata(
        client=civis_client,
        metadata=raw_metadata,
        metadata_paths=upload_metadata_paths,
        filename=metadata_file_name,
    )

    if civis_table_name:
        sql = f"""
                COMMENT ON TABLE {civis_table_name} IS
                \'{clean_metadata["Description"]}\'
                 """
        civis.io.query_civis(
            sql, database=civis_database, polling_interval=2, client=civis_client
        ).result()

    if grant_group:
        sql = f"GRANT ALL ON {civis_table_name} TO GROUP {grant_group}"
        civis.io.query_civis(
            sql, database=civis_database, polling_interval=2, client=civis_client
        ).result()


if __name__ == "__main__":
    DATASET_ID = os.environ["dataset_id"]
    EXISTING_TABLE_ROWS = "drop"
    CLIENT_URL = os.environ["client_url"]
    if "table_name" in list(os.environ.keys()) and "database" in list(
        os.environ.keys()
    ):
        TABLE_NAME = os.environ["table_name"]
        CIVIS_DATABASE = os.environ["database"]
    else:
        TABLE_NAME = None
        CIVIS_DATABASE = None
    if "database_type" in list(os.environ.keys()) and "group" in list(
        os.environ.keys()
    ):
        GRANT_GROUP = os.environ["group"]
        DATABASE_TYPE = os.environ["database_type"]
    else:
        GRANT_GROUP = None
        DATABASE_TYPE = None
    if "socrata_username" in list(os.environ.keys()):
        SOCRATA_USERNAME = os.environ["socrata_username"]
        SOCRATA_PASSWORD = os.environ["socrata_password"]
    else:
        SOCRATA_USERNAME = None
        SOCRATA_PASSWORD = None
    if "varchar_len" in list(os.environ.keys()):
        VARCHAR = os.environ["varchar_len"]
    else:
        VARCHAR = None
    main(
        CLIENT_URL,
        DATASET_ID,
        TABLE_NAME,
        CIVIS_DATABASE,
        DATABASE_TYPE,
        SOCRATA_USERNAME,
        SOCRATA_PASSWORD,
        GRANT_GROUP,
        VARCHAR,
    )
