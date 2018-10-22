import sqlalchemy
import yaml

# Load config file
# with open('config.yml', 'r') as ymlfile:
#     cfg = yaml.load(ymlfile)

def connect_db():
    """ Establish db connection """
    url = 'postgresql://{}:{}@{}:{}/{}'
    url = url.format(pg_conn.login,
                     pg_conn.password,
                     pg_conn.host,
                     pg_conn.port,
                     pg_conn.schema)
    engine = sqlalchemy.create_engine(url)
    return engine

    url = 'postgresql://{}:{}@{}:{}/{}'
    url = url.format(cfg['postgresql']['user'],
                     cfg['postgresql']['pass'],
                     cfg['postgresql']['host'],
                     cfg['postgresql']['port'],
                     cfg['postgresql']['db'])
    engine = sqlalchemy.create_engine(url)
    return engine

def make_tables(testing = True):
    """ Create Postgres tables for dockless data """

    create_types_querystring = """
    DO $$
    BEGIN
        CREATE EXTENSION IF NOT EXISTS postgis;

        IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'event_type') THEN
            CREATE TYPE event_type AS ENUM (
                'available',
                'reserved',
                'unavailable',
                'removed');
        END IF;

        IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'reason') THEN
            CREATE TYPE reason AS ENUM (
            'service_start',
            'maintenance',
            'maintenance_drop_off',
            'rebalance_drop_off',
            'user_drop_off',
            'user_pick_up',
            'low_battery',
            'service_end',
            'rebalance_pick_up',
            'maintenance_pick_up',
            'out_of_service_area_pick_up',
            'out_of_service_area_drop_off');
        END IF;

        IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'vehicle_type') THEN
            CREATE TYPE vehicle_type AS ENUM (
                'bicycle',
                'scooter');
        END IF;

    END$$;
    """

    # Temporary Testing Parameters
    # status_changes: Removing device_id UUID resriction & event_time NOT NULL restriction
    # trips: Removing device_id, trip_id UUID restriction 
    # trips: Removing event_time NOT NULL restriction
    # trips: Removed restriction geometry(LINESTRING, 4326)
    create_tables_querystring_testing = """
    CREATE TABLE IF NOT EXISTS status_changes (
        provider_id UUID NOT NULL,
        provider_name TEXT NOT NULL,
        device_id TEXT NOT NULL, 
        vehicle_id TEXT NOT NULL,
        vehicle_type TEXT NOT NULL,
        propulsion_type TEXT NOT NULL,
        event_type event_type NOT NULL,
        event_type_reason reason,
        event_time DOUBLE PRECISION,
        battery_pct DOUBLE PRECISION,
        associated_trips TEXT,
        event_location geometry(POINT, 4326)
    );

    CREATE TABLE IF NOT EXISTS trips (
        provider_id UUID NOT NULL,
        provider_name TEXT NOT NULL,
        device_id TEXT NOT NULL, 
        vehicle_id TEXT NOT NULL,
        vehicle_type TEXT NOT NULL,
        propulsion_type TEXT NOT NULL,
        trip_id TEXT NOT NULL,
        trip_duration BIGINT NOT NULL,
        trip_distance BIGINT NOT NULL,
        accuracy BIGINT NOT NULL,
        start_time DOUBLE PRECISION NOT NULL,
        end_time DOUBLE PRECISION NOT NULL,
        parking_verification_url TEXT,
        standard_cost DOUBLE PRECISION,
        actual_cost DOUBLE PRECISION,
        route geometry
    );

    CREATE TABLE IF NOT EXISTS trip_routes (
        trip_id TEXT NOT NULL,
        time_update DOUBLE PRECISION,
        geom geometry(POINT, 4326)
    );
    """

    create_tables_querystring = """
    CREATE TABLE IF NOT EXISTS status_change (
        provider_id TEXT NOT NULL,
        provider_name TEXT NOT NULL,
        device_id TEXT NOT NULL,
        vehicle_id TEXT NOT NULL,
        vehicle_type vehicle_type NOT NULL,
        propulsion_type TEXT NOT NULL,
        event_type event_type NOT NULL,
        event_type_reason reason,
        event_time DOUBLE PRECISION NOT NULL,
        battery_pct DOUBLE PRECISION,
        associated_trips TEXT,
        event_location geometry(POINT, 4326)
    );
    """

    engine = connect_db()
    conn = engine.connect()
    conn.execute(create_types_querystring)
    if testing == True:
        conn.execute(create_tables_querystring_testing)
    elif testing == False:
        conn.execute(create_tables_querystring)
    conn.close()

if __name__ == '__main__':
    make_tables()
