# Non-AWS modules
import requests
import yaml
import os
import json
import datetime, time, pytz
from config import config as cfg
# AWS
# create a new bucket

def get_provider_data(provider, feed, start_time=None, end_time=None, testing=True, **kwargs):
    """ Query provider API

    Args:
        provider (str): Name of mobility provider Ex. 'lime'
        feed (str): API Feed. Ex. 'trips', 'status_changes'
        start_time (obj): Python datetime object in PDT tz 
        end_time (obj): Python datetime object in PDT tz 
        **kwargs: Additional query parameters for API endpoint

    Returns:
        JSON dump in directory

    """
    # Set params
    url = cfg.provider[provider][feed]
    headers = {'Authorization': cfg.provider[provider]['auth']}
    params = kwargs
    for lbl, timeval in {'start_time': start_time, 'end_time': end_time}.items():
        if timeval is not None:
            unix_secs = time.mktime(timeval.timetuple())
            params[lbl] = unix_secs
    
    # Initial GET reuest
    r = requests.get(url, headers = headers, params = params)
    if r.status_code != requests.codes.ok:
        print(r.status_code)
        return None
    first_page = r.json()
    provider_data = first_page['data']

    # Paginate through results
    if testing == True:
        i = 0
        next_url = first_page['links']['next']
        while i < 3:
            r = requests.get(next_url, headers = headers, params = params)
            if r.status_code != requests.codes.ok:
                print(r.status_code)
                return None
            next_page = r.json()
            next_url = next_page['links']['next']
            for record in next_page['data']:
                provider_data.append(record)
            i += 1
    
    # Paginate through results
    if testing == False:
        next_url = first_page['links']['next']
        while next_url is not None:
            r = requests.get(next_url, headers = headers, params = params)
            if r.status_code != requests.codes.ok:
                print(r.status_code)
                return None
            next_page = r.json()
            next_url = next_page['links']['next']
            for record in next_page['data']:
                provider_data.append(record)
        
    # TODO: Dump data to S3 bucket
    # Format filename by time quey params
    start_str = "{:04}{:02}{:02}{:02}{:02}".format(*start_time.timetuple()[0:5])
    end_str = "{:04}{:02}{:02}{:02}{:02}".format(*end_time.timetuple()[0:5])
    fname = "{}-{}-{}-{}.json".format(start_str, end_str, provider, feed)
    fpath = os.path.join(os.path.dirname(__file__), fname)
    with open(fname, 'w') as outputfile:
        json.dump(provider_data, outputfile)



if __name__ == '__main__':

    # Testing Time Range: Sept 15 @ 1pm - 3pm
    tz = pytz.timezone("US/Pacific")
    start_time = tz.localize(datetime.datetime(2018, 9, 15, 13))
    end_time = tz.localize(datetime.datetime(2018, 9, 15, 15))

    get_provider_data(provider = 'lime', feed = 'status_changes', start_time = start_time, end_time = end_time)
