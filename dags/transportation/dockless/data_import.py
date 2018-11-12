import requests
import yaml
import os
import json
import datetime, time, pytz
import boto3
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable

provider_conn = Variable.get("provider", deserialize_json=True)

class MDSProviderApi: 
    """ Class representing an MDS provider API """
    def __init__(self, name):
        self.name = self.set_name(name)
        self.baseurl = self.set_url()
        self.token = self.set_token()
        self.headers = self.compose_header()
        self.paginate = False

    def set_name(self, name):
        name = name.lower()
        if name not in provider_conn.keys():
            raise KeyError("Provider {} not in list of providers.".format(name))
        return name

    def set_url(self):
        if 'baseurl' not in provider_conn[self.name].keys():
            raise KeyError("No base url defined for provider {}.".format(self.name))
        baseurl = provider_conn[self.name]['baseurl']
        return baseurl

    def set_token(self):
        if 'token' not in provider_conn[self.name].keys():
            raise KeyError("No token defined for provider {}.".format(self.name))
        token = provider_conn[self.name]['token']
        return token

    def compose_header(self):
        if self.name == 'bird':
            auth = 'Bird ' + self.token
            header = {'Authorization': auth, 'APP-Version': '3.0.0'}
        # for testing purposes only
        elif self.name == 'lemon':
            header = None
        else:
            auth = 'Bearer ' + self.token
            header = {'Authorization': auth}
        return header
    
    def get_data(self, feed, testing, params):
        # Set url, params
        if feed == 'trips':
            url = self.baseurl + '/trips'
        elif feed == 'status_changes':
            url = self.baseurl + '/status_changes'
        else:
            print('Not a valid feed')
            return None

        # Initial request

        r = requests.get(url, headers=self.headers, params=params)

        if r.status_code != requests.codes.ok:
            print(r.status_code)
            return None
        first_page = r.json()

        # for test data
        if self.name == 'lemon':
            provider_data = first_page
            return provider_data
        else:
            provider_data = first_page['data'][feed]
        if 'links' not in first_page.keys():
            return provider_data

        
        # Paginate, if applicable
        if testing == True:
            i = 0
            next_url = first_page['links']['next']
            while i < 2:
                r = requests.get(next_url, headers=self.headers, params=params)
                if r.status_code != requests.codes.ok:
                    print(r.status_code)
                    return None
                next_page = r.json()
                next_url = next_page['links']['next']
                print(next_page)
                for record in next_page['data'][feed]:
                    provider_data.append(record)
                i += 1
        # Paginate, if applicable
        if testing == False:
            next_url = first_page['links']['next']
            while next_url is not None:
                r = requests.get(next_url, headers=self.headers, params=params)
                if r.status_code != requests.codes.ok:
                    print(r.status_code)
                    return None
                next_page = r.json()
                next_url = next_page['links']['next']
                for record in next_page['data'][feed]:
                    provider_data.append(record)
        return provider_data

def connect_aws_s3():
    """ Connect to AWS """
    aws_conn = BaseHook.get_connection('aws_default').extra_dejson 
    session = boto3.Session(
    aws_access_key_id=aws_conn["aws_access_key_id"],
    aws_secret_access_key=aws_conn["aws_secret_access_key"])
    s3 = session.resource('s3')
    return s3

def get_provider_data(provider_name, feed, end_time_gte=None, end_time_lte=None, testing=True, **context):
    """ Query provider API

    Args:
        provider (str): Name of mobility provider Ex. 'lime'
        feed (str): API Feed. Ex. 'trips', 'status_changes'
        start_time (obj): Python datetime object in PDT tz 
        end_time (obj): Python datetime object in PDT tz 
        **context: Additional query parameters for API endpoint

    Returns:
        JSON dump in aws S3 bucket

    """
    # Set params, currently for daily pull
    provider = MDSProviderApi(provider_name)
    period_begin = time.mktime(context['execution_date'].timetuple())
    period_end = period_begin + 86400
    params = {'end_time_gte': period_begin, 'end_time_lte': period_end}

    # Get provider data
    provider_data = provider.get_data(feed, testing, params=params)
    
    # Write to S3 bucket
    fname = "{}-{}-{}-{}.json".format(int(period_begin), int(period_end), provider_name, feed)
    s3 = connect_aws_s3()
    obj = s3.Object('dockless-raw-test', fname)
    obj.put(Body=json.dumps(provider_data))

if __name__ == '__main__':

    get_provider_data(provider_name='lemon', feed='trips')
