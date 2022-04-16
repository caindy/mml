import os
from requests import Session
import pandas as pd
from requests.sessions import HTTPAdapter
import numpy as np
from io import StringIO

from urllib3.util.retry import Retry
from datetime import datetime, date
from time import time

retries = Retry(total=5, backoff_factor=0.1,
                status_forcelist=[500, 502, 503, 504])

def get_station_ids(state : str, start_year: int, s : Session):
    response = s.get(f'https://mesonet.agron.iastate.edu/geojson/network/{state}_ASOS.geojson')
    if response.ok:
        j = response.json()
        sites = [site["properties"]  for site in j["features"]]
        valid_sites = []
        for site in sites:
            try:
                first_year = int(site["time_domain"][1:5])
                if first_year <= start_year and site["time_domain"][6:9].lower() == "now":
                    valid_sites.append(site["sid"])
            except:
                continue
        print(f'Found {len(sites)} valid sites for {state}')
        return valid_sites
    else:
        print(f'Request for {state} failed: {response.status_code} {response.reason}')

def get_station_data(id: str, start: datetime, end: datetime, s : Session):
    asos_url = 'https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py'
    data_query = 'data=tmpf&data=feel'
    date_query = f'year1={start.year}&month1={start.month}&day1={start.day}&year2={end.year}&month2={end.month}&day2={end.day}'
    query = f'?{data_query}&tz=Etc/UTC&format=comma&latlon=yes&{date_query}'
    url = f'{asos_url}{query}&station={id}'
    response = s.get(url)
    if response.ok:
        print(f'Length of response: {len(response.content)}')
        csv = response.content.decode('utf-8')
        print(f'Length of decoded response: {len(csv)}')
        df = pd.read_csv(StringIO(csv), skiprows = 5)
        print(f'Retrieved {df.size} observations for {id}')
        if df.size > 0:
            return df[['station', 'valid', 'tmpf', 'lat', 'lon', 'feel']]
        else:
            print(f'URL {url}')
            print(csv)
            return None
    else:
        print(f'Request for {id} in failed: {response.status_code} {response.reason}')

def get_station_df(sid : str, s : Session, start = datetime(2015,1,20)):
    station = get_station_data(sid, start, date.today(), s)
    if station is None:
        return None

    station['valid'] = pd.to_datetime(station['valid'], utc = True)
    station = station[station['tmpf'] != 'M']
    numeric_cols = ['tmpf', 'lat', 'lon']
    station[numeric_cols] = station[numeric_cols].apply(pd.to_numeric, axis=1)

    station['feel'] = np.where(station['feel'] == 'M', station['tmpf'], station['feel'])
    station['feel'] = station[['feel']].apply(pd.to_numeric, axis=1)

    return station


states = ['ND', 'SD', 'MN', 'IA', 'AR', 'IN', 'IL', 'NE', 'KS', 'MO', 'WI', 'KY', 'LA', 'AR', 'MS']
# additional states to consider: East TX,OH

# with Session() as s:
#     s.mount('http://', HTTPAdapter(max_retries=retries))
#     for state in states:
#         state_df = pd.DataFrame()
#         stations = get_station_ids(state, 2014, s)
#         Parallel(n_jobs=4, prefer="threads")(delayed(write_station_df)(sid, s) for sid in stations) 
            
def get_session(protocol = 'http://'):
    s = Session()
    s.mount('http://', HTTPAdapter(max_retries=retries))
    return s
    

# #    df = pd.DataFrame()
    # stations = {'IA' : ['CBF', 'IKV', 'CWI', 'VTI'],
    #             'MN' : ['DLH', 'JKJ', 'LYV', 'MSP', 'RST'], 
    #             'WI' : ['MSN', 'MKE', 'EAU', 'GRB '],
    #             'MI' : ['ANJ', 'GRR', 'LAN', 'DET', 'ARB'],
    #             'IN' : ['EVV', 'FWA', 'GYY', 'IND', 'SBN', 'SPI'],
    #             'IL' : ['BMI', 'CMI', 'ARR', 'PIA'],
    #             'MO' : ['STL', 'COU', 'SGF', 'MKC'],
    #             'MS' : ['HKS', 'MJD', 'TUP', 'MEI'],
    #             'LA' : ['BTR', 'LFT', 'LCH', 'SHV', 'AEX'],
    #             'TX' : ['LFK'] }

    # stations_df = pd.DataFrame()
    # stations_df = stations_df.append([write_station_df(sid, s) for sid in stations])
    # stations_df.to_parquet('subset_stations.parquet')


