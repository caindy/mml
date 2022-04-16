import pandas as pd
from datetime import datetime, date, timedelta

dir_name = 'DailyRegionalForecasts'

# Checking
#for day in pd.date_range(date.today() - timedelta(days=1), date.today()):
all_data = pd.DataFrame()
#for day in pd.date_range(datetime(2013, 12, 10), date.today()):
for day in pd.date_range(datetime(2013, 12, 20), date.today()):
#for day in pd.date_range(datetime(2009, 7, 7), datetime(2013, 12, 9)):  #date.today()):
    YYYYMMDD = day.strftime('%Y%m%d')
    file_name = f'data/{dir_name}/{YYYYMMDD}_rf_al.xls'
    df = pd.read_excel(file_name, skiprows=5) 
    df = df[1:25]
    old_miso_cols = ['East MTLF (MWh)', 'East ActualLoad (MWh)',
                     'West MTLF (MWh)', 'West ActualLoad (MWh)',
                     'Midwest ISO MTLF (MWh)', 'Midwest ISO ActualLoad (MWh)']
    new_miso_cols = ['North MTLF (MWh)', 'North ActualLoad (MWh)',
                     'South MTLF (MWh)', 'South ActualLoad (MWh)',
                     #'MISO ISO MTLF (MWh)' if day in pd.date_range(datetime(2013,12,10), datetime(2013,12,12)) else 'MISO MTLF (MWh)',
                     'MISO MTLF (MWh)',
                     'MISO ActualLoad (MWh)']
    #miso_cols = old_miso_cols if old_miso_cols[0] in df.columns else new_miso_cols
    numeric_cols = ['HourEnding', 'Central MTLF (MWh)', 'Central ActualLoad (MWh)'] + new_miso_cols
                    
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric)
    df[['Market Day']] = df[['Market Day']].apply(pd.to_datetime)
    df = df[['Market Day'] + numeric_cols]
    if df.iloc[23].HourEnding != 24:
        print(f'Failed to compiled {file_name}')
    else:
        all_data = all_data.append(df)
        print(f'Completed {YYYYMMDD} with {all_data.size} rows')

all_data.to_parquet('all_regional_mtlf_after_intg.parquet')
