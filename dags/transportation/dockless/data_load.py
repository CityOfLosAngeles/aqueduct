from config import config as cfg
from geoalchemy2 import Geometry, Geography, shape, WKTElement
from geoalchemy2 import shape
import sqlalchemy
from sqlalchemy import MetaData, Table
import geopandas as gpd
import pandas as pd
import json
import pytz, datetime, time, os
from shapely import geometry, wkt

def connect_db():
    """ Establish db connection """
    url = 'postgresql://{}:{}@{}:{}/{}'
    url = url.format(cfg.postgresql['user'],
                     cfg.postgresql['pass'],
                     cfg.postgresql['host'],
                     cfg.postgresql['port'],
                     cfg.postgresql['db'])
    print(url)
    engine = sqlalchemy.create_engine(url)
    return engine

def compose_filename(provider, feed, start_time, end_time):
    """ Compose a filename for loading json file """
    start_str = "{:04}{:02}{:02}{:02}{:02}".format(*start_time.timetuple()[0:5])
    end_str = "{:04}{:02}{:02}{:02}{:02}".format(*end_time.timetuple()[0:5])
    fname = "{}-{}-{}-{}.json".format(start_str, end_str, provider, feed)
    fpath = os.path.join(os.path.dirname(__file__), fname)
    return fpath

def load_json(provider, feed, start_time, end_time, testing = False):
    """ Load JSON dump to 

    Args:
        provider (str): Name of mobility provider Ex. 'lime'
        feed (str): API Feed. Ex. 'trips', 'status_changes'
        start_time (obj): Python datetime object in PDT tz 
        end_time (obj): Python datetime object in PDT tz 

    Returns:
        Appends data to existing PostGIS table

    """
    # Load File
    file = compose_filename(provider, feed, start_time, end_time)
    with open(file, 'r') as inputfile:
        json_data = json.load(inputfile)

    if testing == True:
        for row in json_data:
            for coord in row['event_location']['coordinates']:
                index = row['event_location']['coordinates'].index(coord)
                row['event_location']['coordinates'][index] = float(coord)

    for row in json_data:
        row['event_location'] = WKTElement(geometry.shape(row['event_location']).wkt, srid = 4326)

    engine = connect_db()
    provider_df = pd.DataFrame(json_data)
    provider_df.to_sql(feed, engine, if_exists = 'append', index = False, 
                        dtype = {'event_location': Geometry('POINT', srid = 4326)})

if __name__ == '__main__':

    # Testing Time Range: Sept 15 @ 1pm - 3pm
    tz = pytz.timezone("US/Pacific")
    start_time = tz.localize(datetime.datetime(2018, 9, 15, 13))
    end_time = tz.localize(datetime.datetime(2018, 9, 15, 15))

    load_json(provider = 'lime', feed = 'status_changes', start_time = start_time, end_time = end_time)
