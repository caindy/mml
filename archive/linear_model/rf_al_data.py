from datetime import date, datetime, timedelta, timezone
from genericpath import isfile
import pandas as pd
from requests import Session
from multiprocessing import cpu_count
from joblib import Parallel, delayed
from request_session import get_session
import zipfile


# Downloads Daily Regional Forecast and Actual Load (xls)

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
