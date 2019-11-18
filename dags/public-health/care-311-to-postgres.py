from sodapy import Socrata
from itertools import chain
from geoalchemy2 import Geometry, WKTElement

def load_to_postgres():
    """
    Loads the care, care plus response codes 
    to postgres as an upsert
    """


if __name__ == '__main__': 
    dataset_id = 'jvre-2ecm'
    client = Socrata("data.lacity.org",
                     "PrfihrlGM6GihbDbDRHqtQ0R4",
                     username="hunter.owens@lacity.org",
                     password="e*75BTtTP#Qm*c^N8J7y")

    results = []
    req_count = 0
    page_size = 2000
    data = None
    while data != []: 
        data = client.get(dataset_id, # view, limited to correct reason codes 
                          content_type="json",
                          offset=req_count * page_size,
                          limit=page_size)
        req_count+=1 
        results.append(data) 
    df = pd.DataFrame.from_dict(list(chain.from_iterable(results)))
    srid = 4326

    df["geom"] = df.dropna(subset=["latitude", "longitude"]).apply(
        lambda x: WKTElement(Point(x.longitude, x.latitude).wkt, srid=srid), axis=1
    )

    # Create the connection
    connection_string = (
        os.environ.get("POSTGRES_URI")
        or f"postgres://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
    )
    engine = create_engine(connection_string)

        
    # Write the dataframe to the database
    df.to_sql(
        "311-cases-homelessness",
        engine,
        schema="public-health",
        if_exists="replace",
        dtype={"geom": Geometry("POINT", srid=srid)},
    )