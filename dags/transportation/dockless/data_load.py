from geoalchemy2 import Geometry, shape, WKTElement
import sqlalchemy
from sqlalchemy import MetaData, Table, Enum
import geopandas as gpd
import pandas as pd
from pandas.io.json import json_normalize
import json, geojson
import pytz, datetime, time, os
from shapely import geometry, wkt
from uuid import UUID
import yaml
import boto3

# Load config file
with open('config.yml', 'r') as ymlfile:
    cfg = yaml.load(ymlfile)

def connect_db():
    """ Establish db connection """

    url = 'postgresql://{}:{}@{}:{}/{}'
    url = url.format(cfg['postgresql']['user'],
                     cfg['postgresql']['pass'],
                     cfg['postgresql']['host'],
                     cfg['postgresql']['port'],
                     cfg['postgresql']['db'])
    engine = sqlalchemy.create_engine(url)
    return engine

def connect_aws_s3():
    """ Connect to AWS """
    session = boto3.Session(
    aws_access_key_id=cfg['aws']['key_id'],
    aws_secret_access_key=cfg['aws']['key'])
    s3 = session.resource('s3')
    return s3

def compose_filename(provider, feed, start_time, end_time):
    """ Compose a filename for loading json file """

    start_str = "{:04}{:02}{:02}{:02}{:02}".format(*start_time.timetuple()[0:5])
    end_str = "{:04}{:02}{:02}{:02}{:02}".format(*end_time.timetuple()[0:5])
    fname = "{}-{}-{}-{}.json".format(start_str, end_str, provider, feed)
    return fname

    # For local access
    # fpath = os.path.join(os.path.dirname(__file__), fname)
    # return fpath


def build_linestring(geojson):
    """ Compose a linestring from a geojson point feature collection """

    linestring_coords = []
    for feature in geojson['features']:
        linestring_coords.append(feature['geometry']['coordinates'])
    linestring = {'type': 'LineString', 'coordinates': linestring_coords}
    return linestring

def format_trips(json_data, testing=True):
    """ Format trips JSON object to trips & trip_routes DFs """

    # Create new DF for routes
    trip_routes_df = pd.DataFrame()

    # Testing: Corrects for any trips that are < 2
    if testing == True:
        json_data[:] = [row for row in json_data if len(row['route']['features']) > 1]

    # Format trips JSON
    # print(json_data)
    for row in json_data:
        print(row)
        row['provider_id'] = UUID(row['provider_id'])
        row['provider_name'] = str(row['provider_name'])
        row['device_id'] = UUID(row['device_id'])
        row['vehicle_id'] = str(row['vehicle_id'])
        row['vehicle_type'] = str(row['vehicle_type']) 
        row['propulsion_type'] = str(row['propulsion_type']) 
        row['trip_id'] = UUID(row['trip_id'])
        row['trip_duration'] = int(row['trip_duration'])
        row['trip_distance'] = int(row['trip_distance'])
        row['accuracy'] = int(row['accuracy']) 
        row['start_time'] = int(row['start_time']) 
        row['end_time'] = int(row['end_time']) 

        # Optional attributes
        if 'parking_verification_url' in row:
            row['parking_verification_url'] = str(row['parking_verification_url'])
        if 'standard_cost' in row:
            row['standard_cost'] = float(row['standard_cost'])
        if 'actual_cost' in row:
            row['actual_cost'] = float(row['standard_cost'])

        # Format GeoJSON for trip routes table, include trip-id
        route_df = json_normalize(row['route']['features'])
        route_df['trip_id'] = row['trip_id']
        trip_routes_df = trip_routes_df.append(route_df, sort=False)

        # Format simplified route for trips table
        linestring = build_linestring(row['route'])
        row['route'] = WKTElement(geometry.shape(linestring).wkt, srid=4326)

    # JSON to DF
    trips_df = pd.DataFrame(json_data)
    trip_routes_df.drop(['type', 'geometry.type'], axis=1, inplace=True)
    trip_routes_df.rename(columns={'properties.timestamp': 'time_update',
                                   'geometry.coordinates': 'geom'},
                          inplace=True)
    trip_routes_df['geom'] = trip_routes_df['geom'].apply(geometry.Point)
    trip_routes_df['geom'] = trip_routes_df['geom'].apply(WKTElement, srid=4326)

    # Write to db
    write_trips(trips_df, trip_routes_df)

def format_status_changes(json_data, testing=True):
    """ Format status changes JSON object to DF """

    print(json_data)
    # Format status changes JSON
    for row in json_data:
        #print(row)
        row['provider_id'] = UUID(row['provider_id'])
        row['provider_name'] = str(row['provider_name'])
        row['device_id'] = UUID(row['device_id'])
        row['vehicle_id'] = str(row['vehicle_id'])
        row['vehicle_type'] = str(row['vehicle_type']) 
        row['propulsion_type'] = str(row['propulsion_type']) 
        row['event_type'] = str(row['event_type']) 
        row['event_type_reason'] = str(row['event_type_reason']) 
        row['event_time'] = int(row['event_time']) # Timestamp
        if testing==True:
            row['event_location'] = WKTElement(geometry.shape(row['event_location']).wkt, srid=4326)
        elif testing==False:
            row['event_location'] = WKTElement(geometry.shape(row['event_location']['geometry']).wkt, srid=4326)
        
        # Optional attributes
        if 'battery_pct' in row:
            row['battery_pct'] = float(row['battery_pct'])
        if 'associated_trips' in row: 
            row['associated_trips'] = str(row['associated_trips'])

    # JSON to DF
    status_changes_df = pd.DataFrame(json_data)
    print(status_changes_df.shape[0])
    write_status_changes(status_changes_df)

def write_trips(trips_df, route_df):
    """ Write trips & trip_routes DFs to DB """

    # Enum vars
    vehicle_types = ('bicycle', 'scooter')
    vehicle_type_enum = Enum(*vehicle_types, name='vehicle_type')
    trips_dtypes = {'route': Geometry('LINESTRING', srid=4326),
                    'vehicle_type': vehicle_type_enum}
    routes_dtypes = {'geom': Geometry('POINT', srid=4326)}

    # Write to db
    engine = connect_db()
    print('Committing trips to db.')
    trips_df.to_sql('trips', engine, if_exists='append', index=False, dtype=trips_dtypes) 
    print('Successfully committed trips to db.')
    print('Committing routes to db.')
    route_df.to_sql('trip_routes', engine, if_exists='append', index=False, dtype=routes_dtypes)
    print('Successfully committed routes to db.')

def write_status_changes(status_changes_df):
    """ Write status_changes DF to DB """

    # Enum vars
    event_types = ('available', 'reserved', 'unavailable', 'removed')
    reasons = ('service_start', 'user_drop_off', 'rebalance_drop_off', 'maintenance_drop_off',
               'user_pick_up', 'maintenance', 'low_battery', 'service_end', 'rebalance_pick_up',
               'maintenance_pick_up')
    vehicle_types = ('bicycle', 'scooter')
    event_type_enum = Enum(*event_types, name='event_type')
    reason_enum = Enum(*reasons, name='reason')
    vehicle_type_enum = Enum(*vehicle_types, name='vehicle_type')
    dtypes = {'event_location': Geometry('POINT', srid=4326),
              'event_type': event_type_enum,
              'event_type_reason': reason_enum,
              'vehicle_type': vehicle_type_enum}
    
    engine = connect_db()
    print("Committing status changes to db.")
    status_changes_df.to_sql('status_changes', engine, if_exists='append', index=False, dtype=dtypes)
    print('Successfully committed trips to db.')

def load_json(provider, feed, start_time, end_time, testing=False):
    """ Load JSON dump to db

    Args:
        provider (str): Name of mobility provider Ex. 'lime'
        feed (str): API Feed. Ex. 'trips', 'status_changes'
        start_time (obj): Python datetime object in PDT tz 
        end_time (obj): Python datetime object in PDT tz 

    Returns:
        Commits clean table to Postgresql db

    """

    # Open file
    fname = compose_filename(provider, feed, start_time, end_time)

    # # For local file access
    # with open(fname, 'r') as inputfile:
    #     json_data = json.load(inputfile)

    # Connect to S3 bucket
    s3 = connect_aws_s3()
    obj = s3.Object('dockless-raw-test', fname)
    json_file = obj.get()['Body'].read().decode('utf-8')
    json_data = json.loads(json_file)

    # Transform, Load
    if feed == 'trips':
        format_trips(json_data)
    if feed == 'status_changes':
        format_status_changes(json_data)

if __name__ == '__main__':

    # Testing Time Range: Oct 8-12 2018 PDT
    tz = pytz.timezone("US/Pacific")
    start_time = tz.localize(datetime.datetime(2018, 10, 9))
    end_time = tz.localize(datetime.datetime(2018, 10, 10))

    load_json(provider='lemon', feed='trips', start_time=start_time, end_time=end_time)
