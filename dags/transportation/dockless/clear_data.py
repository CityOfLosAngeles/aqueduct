import sqlalchemy
import yaml
import datetime, time, pytz

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

def clear_data(testing, end_time_gte, end_time_lte):
    """ Clear dockless data within time range"""

    clear_data_querystring = """
    DELETE FROM trips 
    WHERE end_time > {} AND end_time < {};
    """.format(end_time_gte, end_time_lte)

    # TODO: Need to clear trip_routs as well

    engine = connect_db()
    conn = engine.connect()
    conn.execute(clear_data_querystring)
    conn.close()

if __name__ == '__main__':
    
    # Testing Time Range: Oct 8-12 2018 PDT
    tz = pytz.timezone("US/Pacific")
    start_time = tz.localize(datetime.datetime(2018, 10, 9))
    start_time = time.mktime(start_time.timetuple())
    end_time = tz.localize(datetime.datetime(2018, 10, 10))
    end_time = time.mktime(end_time.timetuple())

    testing = True

    clear_data(testing, start_time, end_time)
