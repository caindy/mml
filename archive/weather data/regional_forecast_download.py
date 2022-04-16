from requests import Session
import pandas as pd
from datetime import datetime, date
import zipfile
import os

# Archived Daily Regional Forecast and Actual Load
first = 'https://docs.misoenergy.org/marketreports/200907_rf_al_xls.zip'
last = 'https://docs.misoenergy.org/marketreports/201812_rf_al_xls.zip'
dir_name = 'DailyRegionalForecasts'

#TODO: download and extract
with Session() as s:
    month_starts = pd.date_range(datetime(2009, 7, 1), datetime(2018, 12, 1), freq='MS')
    for ms in month_starts:
        YYYYMM = ms.strftime('%Y%m')
        url = f'https://docs.misoenergy.org/marketreports/{YYYYMM}_rf_al_xls.zip'
        print(f'Fetch {url}')
        response = s.get(url)
        zip_file = f'data/ArchivedRF/{YYYYMM}.zip'
        with open(zip_file, 'wb') as f:
            f.write(response.content)
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            zip_ref.extractall(dir_name)

with Session() as s:
# Daily Regional Forecast and Actual Load (xls)
    for day in pd.date_range(datetime(2019, 1, 1), date.today()):
        YYYYMMDD = day.strftime('%Y%m%d')
        file_name = f'{YYYYMMDD}_rf_al.xls'
        url = f'https://docs.misoenergy.org/marketreports/{file_name}'
        response = s.get(url)
        with open(f'{dir_name}/{file_name}', 'wb') as f:
            f.write(response.content)


# Checking
for day in pd.date_range(datetime(2009, 7, 7), date.today()):
    YYYYMMDD = day.strftime('%Y%m%d')
    file_name = f'{YYYYMMDD}_rf_al.xls'
    if not os.path.isfile(f'{dir_name}/{file_name}'):
        print(f'{file_name} does not exists')
