import zipfile
from datetime import date, datetime, timedelta, timezone
from io import StringIO
from multiprocessing import cpu_count

import pandas as pd
from genericpath import isfile
from joblib import Parallel, delayed
from pytz import utc
from requests import Session
from requests.sessions import HTTPAdapter
from urllib3.util.retry import Retry

# Downloads Daily Regional Forecast and Actual Load (xls)

# The MISO website keeps older data in a different location than more recent data
tzet = timezone(timedelta(hours=-5))
def prevailing_time(yyyy, mm, dd, hh):
    return datetime(yyyy, mm, dd, hh, tzinfo=tzet)
archive_cutoff = datetime(date.today().year - 3, 12, 31, tzinfo=tzet)

parallel = Parallel(n_jobs=cpu_count())

def get_archive_rf_al(start_date, output_dir, s : Session, end_date = archive_cutoff):
    month_starts = pd.date_range(start_date, end_date, freq='MS')
    for ms in month_starts:
        YYYYMM = ms.strftime('%Y%m')
        url = f'https://docs.misoenergy.org/marketreports/{YYYYMM}_rf_al_xls.zip'
        response = s.get(url)
        zip_file = f'{output_dir}/{YYYYMM}.zip'
        with open(zip_file, 'wb') as f:
            f.write(response.content)
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            zip_ref.extractall(output_dir)

def get_file_name(day):
    YYYYMMDD = day.strftime('%Y%m%d')
    return f'{YYYYMMDD}_rf_al.xls'

def download_rf_al(output_dir, file_name, s):
    output_path = f'{output_dir}/{file_name}'
    if not isfile(output_path):
        url = f'https://docs.misoenergy.org/marketreports/{file_name}'
        response = s.get(url)
        with open(output_path, 'wb') as f:
            f.write(response.content)
    return output_path

def get_daily_rf_al(start_date, end_date, output_dir, s : Session):
    all_days = pd.date_range(start_date, end_date)
    return parallel(delayed(download_rf_al)(output_dir, get_file_name(day), s) for day in all_days)

def get_df_for_path(p):
    df = pd.read_excel(p, skiprows=5) 
    df = df[1:25]
    new_miso_cols = ['North MTLF (MWh)', 'North ActualLoad (MWh)',
                    'South MTLF (MWh)', 'South ActualLoad (MWh)',
                    'MISO MTLF (MWh)',
                    'MISO ActualLoad (MWh)']
    numeric_cols = ['HourEnding', 'Central MTLF (MWh)', 'Central ActualLoad (MWh)'] + new_miso_cols
                    
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric)
    df[['Market Day']] = df[['Market Day']].apply(pd.to_datetime)
    df = df[['Market Day'] + numeric_cols]
    assert df.iloc[23].HourEnding == 24
    return df

def get_daily_rf_al_df(start_date, end_date, output_dir):
    td : timedelta = end_date - start_date
    paths = [f'{output_dir}/{get_file_name(start_date + timedelta(days =d))}' for d in range(1, td.days)]

    with get_session() as s:
        if start_date < archive_cutoff:
            get_archive_rf_al(start_date, output_dir, s)
        begin = start_date if start_date <= archive_cutoff else archive_cutoff + timedelta(days=1)
        get_daily_rf_al(begin, end_date, output_dir, s)

    dfs = [get_df_for_path(p) for p in paths]

    return pd.concat(dfs)




from geopy import distance
from functools import cached_property, cache

class ASOS():
    """Pandas Adapter for the Iowa State ASOS Network downloads JSON API"""
    def __init__(self, first_year = 2016) -> None:
        self.first_year = first_year or 2016

    miso_states = ['AR', 'IL', 'IN', 'IA', 'KY', 'LA', 'MI', 'MN',
                   'MS', 'MO', 'MT', 'ND', 'SD', 'TX', 'WI']

    __retries = Retry(total=5, backoff_factor=0.1,
                    status_forcelist=[500, 502, 503, 504])

    @cached_property
    def session(self):
        s = Session()
        s.mount('https://', HTTPAdapter(max_retries=ASOS.__retries))
        return s

    @cached_property
    def stations(self):
        return pd.concat([ASOS.__get_stations(state, self.first_year, self.session)
                            for state in ASOS.miso_states])

    @staticmethod
    def __get_stations(state, start_year, s : Session):
        response = s.get(f'https://mesonet.agron.iastate.edu/geojson/network/{state}_ASOS.geojson')
        if response.ok:
            j = response.json()
            sites = [site["properties"] for site in j["features"]]
            for (i, site) in enumerate(sites):
                coords = j['features'][i]['geometry']['coordinates']
                site['latlon'] = (coords[1], coords[0])
            valid_sites = []
            for site in sites:
                try:
                    first_year = int(site["time_domain"][1:5])
                    if first_year <= start_year and site["time_domain"][6:9].lower() == "now":
                        valid_sites.append(site)
                except:
                    continue
            return pd.DataFrame(valid_sites)
        else:
            raise f'Request for {state} failed: {response.status_code} {response.reason}'

    def get_nearest_stations(self, sid: str, radius_miles = 20):
        stations = self.stations.copy()
        target = stations[stations['sid'] == sid].iloc[0]['latlon']
        stations['distance'] = stations['latlon'].apply(lambda c: distance.distance(target, c).mi)
        stations = stations[stations['distance'] < radius_miles]
        stations.sort_values(by=['distance'], inplace=True)
        return stations[1:]['sid'].array

    @staticmethod
    def get_station_csv(id: str, start: datetime, end: datetime, s : Session):
        asos_url = 'https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py'
        data_query = 'data=tmpf&data=feel'
        start = start.astimezone(tz=utc) 
        end = end.astimezone(tz=utc) + timedelta(days=1)
        date_query = f'year1={start.year}&month1={start.month}&day1={start.day}&year2={end.year}&month2={end.month}&day2={end.day}'
        query = f'?{data_query}&tz=Etc/UTC&format=comma&latlon=yes&{date_query}'
        url = f'{asos_url}{query}&station={id}'
        print(f'Fetching {url}')
        response = s.get(url)
        if response.ok:
            return response.content.decode('utf-8')
        raise f'Request failed for {url}'

    def get_station_data(self, id: str, start: datetime, end: datetime) -> pd.DataFrame:
        # we want extra data on ends for interpolation over lacunae
        buffer = timedelta(days=1)
        start_utc = start.astimezone(tz=utc) - buffer
        end_utc = end.astimezone(tz=utc) + buffer

        csv = ASOS.get_station_csv(id, start_utc, end_utc, self.session)
        df = pd.read_csv(StringIO(csv), skiprows = 5)
        if df.size < 1:
            raise f'Error parsing {csv}'

        df['idx'] = pd.to_datetime(df['valid'], utc=True)
        df['observation_time'] = df['idx']
        df['temp'] = pd.to_numeric(df['tmpf'], errors='coerce')
        df = df.set_index('idx')
        utc_hours = pd.date_range(start_utc, end_utc, freq='H')
        df = df.reindex(utc_hours, method='nearest', tolerance=timedelta(minutes=15))
        df['temp'] = df['temp'].interpolate(method='spline', order=3)
        df = df[start.astimezone(tz=utc):end.astimezone(tz=utc)]
        df['hour'] = pd.date_range(start, end, freq='H')
        return df[['station', 'observation_time', 'temp', 'hour']].set_index('hour')
        

    #@cache
    def get_station_df(self, sid : str, start_date, end_date):
            df = self.get_station_data(sid, start_date, end_date)
            return df
            nearby = iter(self.get_nearest_stations(sid))
            for nearby in self.get_nearest_stations(sid):
                print(f'trying {nearby}')
                other = self.get_station_data(nearby, start_date, end_date)
                df = df.fillna(other)
                if not any(pd.isna(df['tmpf'])):
                    return df
            return df
    

    def align(w: pd.DataFrame, observation_hours):
        df = w[w['tmpf'] != 'M'].copy()
        #df['valid'] = pd.to_datetime(df['valid'], utc=True)
        #numeric_cols = ['tmpf', 'lat', 'lon']
        #df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, axis=1)
        df = df.drop(columns=['feel'])
        idx = df.drop_duplicates('valid').set_index('valid').index.get_indexer(observation_hours, method='nearest')
        df = df.iloc[idx]
        return df.drop_duplicates('valid')
