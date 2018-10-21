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

def clear_data(provider_name, feed, **context):
    """ Clear dockless data within time range"""

    period_begin = time.mktime(context['execution_date'].timetuple())
    period_end = period_begin + 86400

    clear_data_querystring = """
    DELETE FROM {}
    WHERE provider_name = '{}' AND
    end_time > {} AND end_time < {};
    """.format(feed, provider_name, period_begin, period_end)

    # TODO: Need to clear trip_routes as well
    # or just setup cascade delete in SQL
    engine = connect_db()
    conn = engine.connect()
    conn.execute(clear_data_querystring)
    conn.close()

if __name__ == '__main__':
    
    provider_name = 'lemon'
    feed = 'trips'
    clear_data(provider_name, feed)
