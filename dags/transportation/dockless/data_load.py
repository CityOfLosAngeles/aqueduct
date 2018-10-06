from geoalchemy2 import Geometry, shape, WKTElement
import sqlalchemy
from sqlalchemy import MetaData, Table, Enum
import geopandas as gpd
import pandas as pd
import json, geojson
import pytz, datetime, time, os
from shapely import geometry, wkt
from uuid import UUID
# Config files
from config import config as cfg

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

def build_linestring(geojson):
    """ Compose a linestring from a geojson point feature collection """
    linestring_coords = []
    for feature in geojson['features']:
        linestring_coords.append(feature['geometry']['coordinates'])
    linestring = {'type': 'LineString', 'coordinates': linestring_coords}
    return linestring

def format_json(json_data, feed, testing = True):
    """ Format JSON for DB, based on MDS Specifications

    Args:
        json_data (obj): JSON object
        feed (str): API Feed. Ex. 'trips', 'status_changes'

    Returns:
        Properly formatted data for table ingestion

    """
    # Enum vars
    event_types = ('available', 'reserved', 'unavailable', 'removed')
    reasons = ('service_start', 'user_drop_off', 'rebalance_drop_off', 'maintenance_drop_off',
               'user_pick_up', 'maintenance', 'low_battery', 'service_end', 'rebalance_pick_up',
               'maintenance_pick_up')
    vehicle_types = ('bicycle', 'scooter')
    event_type_enum = Enum(*event_types, name = 'event_type')
    reason_enum = Enum(*reasons, name = 'reason')
    vehicle_type_enum = Enum(*vehicle_types, name = 'vehicle_type')    

    # Status Changes
    if feed == 'status_changes':
        for row in json_data:
            print(row)
            row['provider_id'] = UUID(row['provider_id'])
            row['provider_name'] = str(row['provider_name'])
            if testing == True:
                row['device_id'] = str(row['device_id'])
            elif testing == False:
                row['device_id'] = UUID(row['device_id'])
            row['vehicle_id'] = str(row['vehicle_id'])
            row['vehicle_type'] = str(row['vehicle_type']) 
            row['propulsion_type'] = str(row['propulsion_type']) 
            row['event_type'] = str(row['event_type']) 
            row['event_type_reason'] = str(row['event_type_reason']) 
            if testing == False:
                row['event_time'] = int(row['event_time']) # Timestamp
            row['event_location'] = WKTElement(geometry.shape(row['event_location']).wkt, srid = 4326)
            # Optional attributes
            # row['battery_pct'] = 
            # row['associated_trips'] =

        dtypes = {'event_location': Geometry('POINT', srid = 4326),
                  'event_type': event_type_enum,
                  'event_type_reason': reason_enum,
                  'vehicle_type': vehicle_type_enum
                  }

    # Trips
    elif feed == 'trips':
        for row in json_data:
            row['provider_id'] = UUID(row['provider_id'])
            row['provider_name'] = str(row['provider_name'])
            if testing == True:
                row['device_id'] = str(row['device_id'])
            elif testing == False:
                row['device_id'] = UUID(row['device_id'])
            row['vehicle_id'] = str(row['vehicle_id'])
            row['vehicle_type'] = str(row['vehicle_type']) 
            row['propulsion_type'] = str(row['propulsion_type']) 
            if testing == True:
                row['trip_id'] = str(row['trip_id'])
            elif testing == False:
                row['trip_id'] = UUID(row['trip_id'])
            row['trip_duration'] = int(row['trip_duration'])
            row['trip_distance'] = int(row['trip_distance'])
            linestring = build_linestring(row['route'])
            row['route'] = WKTElement(geometry.shape(linestring).wkt, srid = 4326)
            row['accuracy'] = int(row['accuracy']) 
            row['start_time'] = int(row['start_time']) 
            row['end_time'] = int(row['end_time']) 
            # Optional attributes
            # row['parking_verification_url'] = 
            # row['standard_cost'] = 
            # row['actual_cost'] =

        dtypes = {'route': Geometry('LINESTRING', srid = 4326),
                  'vehicle_type': vehicle_type_enum
                  }

    return json_data, dtypes

def load_json(provider, feed, start_time, end_time, testing = False):
    """ Load JSON dump to db

    Args:
        provider (str): Name of mobility provider Ex. 'lime'
        feed (str): API Feed. Ex. 'trips', 'status_changes'
        start_time (obj): Python datetime object in PDT tz 
        end_time (obj): Python datetime object in PDT tz 

    Returns:
        Appends data to existing PostGIS table

    """

    # Open file
    file = compose_filename(provider, feed, start_time, end_time)
    with open(file, 'r') as inputfile:
        json_data = json.load(inputfile)

    # Transform
    json_data, dtypes = format_json(json_data, feed = feed)
    # return None

    # Load into DB
    engine = connect_db()
    provider_df = pd.DataFrame(json_data)
    print(provider_df)
    print('committing to db')
    provider_df.to_sql(feed, engine, if_exists = 'append', index = False, dtype = dtypes) 
                        # dtype = {'event_location': Geometry('POINT', srid = 4326),
                        #          'event_type': event_type_enum,
                        #          'event_type_reason': reason_enum,
                        #          'vehicle_type': vehicle_type_enum
                        #          })
    print('committed to db')

if __name__ == '__main__':

    # Testing Time Range: Sept 15 @ 1pm - 3pm
    tz = pytz.timezone("US/Pacific")
    start_time = tz.localize(datetime.datetime(2018, 9, 15, 13))
    end_time = tz.localize(datetime.datetime(2018, 9, 15, 15))

    load_json(provider = 'lime', feed = 'trips', start_time = start_time, end_time = end_time)
