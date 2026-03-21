import requests
import json
import pandas as pd

# full_name = input('What is your full name?')
# email = input('What is your e-mail address?')
api_key = input('What is your NSRDB api key?')

choice = 2
if choice == 1:
    url_start = 'https://developer.nlr.gov/api/solar'\
        + '/nsrdb_data_query.json'

    payload = {'api_key': api_key, 'lat': 39.1385, 'lon': -77.2155}
    r = requests.get(url=url_start, params=payload)
    my_data = r.json()
    with open('practice_location_details.json', 'w') as writer:
        json.dump(my_data, writer)

elif choice == 2:
    url_start = 'https://developer.nlr.gov/api/nsrdb/v2/solar/'\
        + 'nsrdb-GOES-aggregated-v4-0-0-download.csv'

    payload = {'names': 2015,
               'wkt': 'POINT(-77.2155 39.1385)',
               'interval': 60,
               'attributes': 'dhi',
               'utc': 'false',
               'leap_day': 'true',
               'api_key': api_key,
               'email': r'ch83baker@gmail.com'}
    r = requests.get(url=url_start, params=payload)
    with open('practice_dhi_data.csv', 'w') as writer:
        writer.writelines(r.text)
