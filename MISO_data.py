regions = {'North'   : ['IA', 'MN', 'MT', 'ND', 'SD'],
           'Central' : ['IL', 'IN', 'KY', 'MI', 'MO', 'WI'],
           'South'   : ['AR', 'LA', 'MS', 'TX']}
stations_dict = {'IA' : ['DSM', 'CID'], 
                 'MN' : ['DLH', 'JKJ', 'LYV', 'MSP', 'RST'], 
                 'WI' : ['MSN', 'MKE', 'EAU', 'GRB'],
                 'MI' : ['ANJ', 'GRR', 'LAN', 'DET', 'ARB'],
                 'IN' : ['EVV', 'FWA', 'IND', 'SBN', 'SPI'],
                 'IL' : ['BMI', 'CMI', 'ARR', 'PIA'],
                 'MO' : ['STL', 'COU', 'SGF', 'MKC'],
                 'MS' : ['HKS', 'TUP', 'MEI'],
                 'LA' : ['BTR', 'LFT', 'LCH', 'SHV', 'AEX'],
                 'TX' : ['LFK'] }
miso_states = ['AR', 'IL', 'IN', 'IA', 'KY', 'LA', 'MI', 'MN',
               'MS', 'MO', 'MT', 'ND', 'SD', 'TX', 'WI']


import pandas as pd

MISO_PREDICTION_COLUMN_NAME : str = 'MISO MTLF (MWh)'
TARGET_NAME : str = 'MISO ActualLoad (MWh)'

__data__ = pd.read_parquet('./archive/linear_model/data/harmonized_data.parquet')
WEATHER_STATIONS = __data__.columns[0:38]

def prior_load_colname(i : int):
    return f"Actual Load {i} hours prior"

def add_prior_load_features(X, y, num_hours_prior = 36):
    new_features = []
    for i in range(num_hours_prior, 0, -1):
        col_name = prior_load_colname(i)
        X[col_name] = y.shift(i)
        new_features.append(col_name)
    return (X, new_features)

def get_data():
    data = __data__
    pd.set_option('mode.chained_assignment',  None)

    target_name = 'MISO ActualLoad (MWh)'
    y = data[target_name]
    feature_names = [col for col in data.columns if not col.endswith('(MWh)') and not col == 'Market Day']
    X = data[feature_names]
    X['Day of Year'] = data['Market Day'].dt.day_of_year
    feature_names.append('Day of Year')

    num_hours_prior=36
    (X, new_features) = add_prior_load_features(X, y, num_hours_prior)
    feature_names.append(new_features)
    data = X[num_hours_prior:].join(y[num_hours_prior:])

    mtlf = pd.read_parquet('./archive/linear_model/data/actuals_mtlf.parquet')
    data = data.join(mtlf[[MISO_PREDICTION_COLUMN_NAME,'time_idx']].set_index('time_idx'), how='inner')

    return data


from joblib import Parallel, delayed
from datetime import date, datetime, timedelta, timezone
from multiprocessing import cpu_count
from requests import Session
import zipfile
from genericpath import isfile
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

    __retries = Retry(total=5, backoff_factor=0.1,
                    status_forcelist=[500, 502, 503, 504])
    with ession() as s:
        if start_date < archive_cutoff:
            get_archive_rf_al(start_date, output_dir, s)
        begin = start_date if start_date <= archive_cutoff else archive_cutoff + timedelta(days=1)
        get_daily_rf_al(begin, end_date, output_dir, s)

    dfs = [get_df_for_path(p) for p in paths]

    return pd.concat(dfs)