# Basic script to move specific tables from 'public' to another schema using a binding library.
# Script makes use of environmental variables for protecting database login credentials.

import psycopg2
from psycopg2 import sql
import os
from dotenv import load_dotenv
import datetime

env_path = '/home/rmk0110/.env'
load_dotenv(dotenv_path=env_path)

PG_DBNAME = os.getenv("PG_DBNAME")
PG_HOST = os.getenv("PG_HOST")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_PORT = os.getenv("PG_PORT")

conn_str = None

try:
    conn_str = psycopg2.connect(dbname=PG_DBNAME, host=PG_HOST, user=PG_USER, password=PG_PASSWORD, port=PG_PORT)
    print ("Successfully connected to the database.")

except:
    print ("The connection to the database failed.")

start = datetime.datetime.now()
cur = conn_str.cursor()
cur.execute("""alter table if exists public.status_changes set schema mds""")
cur.execute("""alter table if exists public.trips set schema mds""")
conn_str.commit()
cur.execute("""alter materialized view if exists public.v_trips set schema mds""")
cur.execute("""alter materialized view if exists public.v_status_changes set schema mds""")
cur.execute("""alter materialized view if exists public.v_event_types set schema mds""")
conn_str.commit()
# close the cursor
cur.close()
print ("Successfully moved tables and materialized views to the new schema.")
finish = datetime.datetime.now()
elapse = finish - start
secs = elapse.total_seconds()
mins = int(secs / 60) % 60
print ("Elapse time: ",mins,":",elapse.seconds,":",elapse.microseconds*0.001)

if conn_str is not None:
    conn_str.close()
    print ("The connection to the database is closed.")
