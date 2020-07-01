import cx_Oracle
import csv
from io import StringIO
import boto3
import os


dsn = f"""
(DESCRIPTION =
  (ADDRESS_LIST =
    (ADDRESS =
        (PROTOCOL = tcps)
        (HOST = {os.environ.get('ORACLE_HOST')} )
        (PORT = {os.environ.get('ORACLE_PORT')} )
    )
  )
  (CONNECT_DATA =
    (SERVER = DEDICATED)
    (SERVICE_NAME = {os.environ.get('ORACLE_DBNAME')} )
  )
)
"""

target_bucket = "city-of-los-angeles-data-lake"
target_prefix = "ba"
s3 = boto3.client("s3")


def get_column_names(cursor):
    """Extracts the column names from a database cursor."""
    return [c[0] for c in cursor.description]


def save_to_tsv(cursor, table_name, batch_size=100000):
    """Saves rows from a database cursor to a tab-delimited file."""
    print(f"Saving {table_name} to S3")
    part_number = 0
    while True:
        rows = cursor.fetchmany(batch_size)
        if rows == []:
            break
        column_names = get_column_names(cursor)
        f = StringIO()
        writer = csv.writer(f, delimiter="\t")
        writer.writerow(column_names)
        print(f"Fetching data")
        print(f"{table_name} part {part_number}: {len(rows)} rows")
        target_key = (
            f"{target_prefix}/{table_name}/{table_name}_PART_{part_number:06}.tsv"
        )
        writer.writerows(rows)
        print(f"Saving s3://{target_bucket}/{target_key}")
        s3.put_object(Bucket=target_bucket, Key=target_key, Body=f.getvalue())
        part_number += 1
    print("Done")


def main():
    connection = cx_Oracle.connect(
        user=os.environ.get("ORACLE_BAVN_USERNAME"),
        password=os.environ.get("ORACLE_BAVN_PASSWORD"),
        dsn=dsn,
    )
    cursor = connection.cursor()
    table_names = [
        "VW_COMPANY_ALL_NAICS_CODES_NO_ADDRESS",
        "VW_BAVN_COMP_EXTENDED_DETAIL_NO_ADDRESS",
        "VW_COMPANY_ALL_CERTS_NO_ADDRESS",
    ]

    for table_name in table_names:
        query = " ".join(
            f"""
            select
              *
            from
              BAVN.{table_name}@BAVN6.world
        """.strip().split()
        )
        cursor.execute(query)
        save_to_tsv(cursor, table_name)


if __name__ == "__main__":
    main()
