import os

import geopandas
import intake
import sqlalchemy
from intake_civis.alchemy import get_postgres_engine

if os.environ.get("DEV"):
    engine = sqlalchemy.create_engine(os.environ.get("POSTGRES_URI"))
else:
    engine = get_postgres_engine()


def load_dataset(name: str, source: intake.Source, schema: str = "geohub") -> None:
    """
    Load an intake source into postgis.

    Parameters
    ==========
    name: str
        The name of the target table.
    source: intake.Source
        The intake source.
    schema: str
        The schema into which to load the dataset.
    """
    df = source.read()
    if isinstance(df, geopandas.GeoDataFrame):
        df.to_postgis(name, engine, schema=schema, if_exists="replace")
    else:
        df.to_sql(name, engine, schema=schema, if_exists="replace")


if __name__ == "__main__":
    """
    The main entrypoint for the job.
    """
    SCHEMA = os.environ.get("SCHEMA") or "geohub"

    CATALOG_PATH = os.environ.get("CATALOG_PATH") or os.path.join(
        os.path.dirname(__file__), "catalog.yml"
    )

    DCAT_URL = os.environ.get("DCAT_URL") or "geohub.lacity.org/data.json"
    ITEM_ID = os.environ.get("ITEM_ID")
    TABLE_NAME = os.environ.get("TABLE_NAME")

    # Create the schema if it does not exist
    if not engine.dialect.has_schema(engine, SCHEMA):
        engine.execute(sqlalchemy.schema.CreateSchema(SCHEMA))

    # Load/construct the intake catalog to read
    if ITEM_ID and DCAT_URL and TABLE_NAME:
        # If provided with an item ID, DCAT URL, and table name,
        # construct a catalog from those.
        from intake_dcat import DCATCatalog

        catalogs = [
            DCATCatalog(
                DCAT_URL,
                name=SCHEMA,
                items={TABLE_NAME: ITEM_ID},
                priority=["shapefile", "geojson", "csv"],
            ),
        ]
    elif CATALOG_PATH:
        # If provided with a YAML catalog, use that.
        catalogs = [
            val.get()
            for key, val in intake.open_catalog(CATALOG_PATH).items()
            if val.container == "catalog"
        ]
    else:
        # Otherwise, there was an error in the provided parameters.
        raise ValueError(
            "Must provide a catalog path, or an item ID, DCAT URL, and table name"
        )

    exceptions = []
    for catalog in catalogs:
        for name, entry in catalog.items():
            print(f"Loading {name}...", end="", flush=True)
            try:
                load_dataset(name, entry.get(), SCHEMA)
                print("done", flush=True)
            except Exception as e:
                exceptions.append((name, e))
                print("error", flush=True)

    if len(exceptions):
        msg = "\n".join([f"{e[0]}: {e[1]}" for e in exceptions])
        raise RuntimeError(f"Failed to load the following datasets:\n{msg}")
