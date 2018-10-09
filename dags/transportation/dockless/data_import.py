import requests
import yaml
import os
import json
import datetime, time, pytz
# AWS: Create bucket

# Load config file
with open('config.yml', 'r') as ymlfile:
    cfg = yaml.load(ymlfile)

class MDSProviderApi: 
    """ Class representing an MDS provider API """
    def __init__(self, name):
        self.name = self.set_name(name)
        self.baseurl = self.set_url()
        self.token = self.set_token()
        self.headers = self.compose_header()
        self.paginate = False #hmm

    def set_name(self, name):
        name = name.lower()
        if name not in cfg['provider'].keys():
            raise KeyError("Provider {} not in list of providers.".format(name))
        return name

    def set_url(self):
        if 'baseurl' not in cfg['provider'][self.name].keys():
            raise KeyError("No base url defined for provider {}.".format(self.name))
        baseurl = cfg['provider'][self.name]['baseurl']
        return baseurl

    def set_token(self):
        if 'token' not in cfg['provider'][self.name].keys():
            raise KeyError("No token defined for provider {}.".format(self.name))
        token = cfg['provider'][self.name]['token']
        return token

    def compose_header(self):
        if self.name == 'bird':
            auth = 'Bird ' + self.token
            header = {'Authorization': auth, 'APP-Version': '3.0.0'}
        else:
            auth = 'Bearer ' + self.token
            header = {'Authorization': auth}
        return header
    
    def get_data(self, feed, testing=True, **kwargs):

        # Set url, params
        if feed == 'trips':
            url = self.baseurl + '/trips'
        elif feed == 'status_changes':
            url = self.baseurl + '/status_changes'
        else:
            print('Not a valid feed')
            return None
        params = kwargs

        # Initial request
        r = requests.get(url, headers = self.headers, params = params)
        if r.status_code != requests.codes.ok:
            print(r.status_code)
            return None
        first_page = r.json()
        provider_data = first_page['data']
        if 'links' not in first_page.keys():
            return provider_data
        
        # Paginate, if applicable
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
        # Paginate, if applicable
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
        return provider_data

def get_provider_data(provider_name, feed, start_time=None, end_time=None, testing=True, **kwargs):
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
    provider = MDSProviderApi(provider_name)
    params = kwargs
    for lbl, timeval in {'start_time': start_time, 'end_time': end_time}.items():
        if timeval is not None:
            unix_secs = time.mktime(timeval.timetuple())
            params[lbl] = unix_secs
    provider_data = provider.get_data(feed, params)
    
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

    get_provider_data(provider = 'lime', feed = 'trips', start_time = start_time, end_time = end_time)
