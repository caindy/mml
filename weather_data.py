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
archive_cutoff = datetime(date.today().year - 3, 12, 31, tzinfo=timezone(timedelta(hours=-5)))

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

retries = Retry(total=5, backoff_factor=0.1,
                status_forcelist=[500, 502, 503, 504])

def get_session(protocol = 'http://'):
    s = Session()
    s.mount(protocol, HTTPAdapter(max_retries=retries))
    return s

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

def get_station_csv(id: str, start: datetime, end: datetime, s : Session):
    asos_url = 'https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py'
    data_query = 'data=tmpf&data=feel'
    start = start.astimezone(tz=utc) 
    end = end.astimezone(tz=utc)
    date_query = f'year1={start.year}&month1={start.month}&day1={start.day}&year2={end.year}&month2={end.month}&day2={end.day}'
    query = f'?{data_query}&tz=Etc/UTC&format=comma&latlon=yes&{date_query}'
    url = f'{asos_url}{query}&station={id}'
    response = s.get(url)
    if response.ok:
        return response.content.decode('utf-8')
    else:
        return None

def get_station_data(id: str, start: datetime, end: datetime, s : Session):
    if csv := get_station_csv(id, start, end, s):
        df = pd.read_csv(StringIO(csv), skiprows = 5)
        if df.size > 0:
            return df[['station', 'valid', 'tmpf', 'lat', 'lon', 'feel']]
        else:
            print(csv)
    return None

def get_station_df(sid : str, start_date, end_date):
    station = None
    with get_session() as s:
        station = get_station_data(sid, start_date, end_date, s)
        if station is None:
            return None
    return station
