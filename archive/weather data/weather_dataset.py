import pandas as pd
import os

df = pd.DataFrame()
for file in os.listdir('data/weather'):
    if file.endswith('weather_data.parquet'):
        df = df.append(pd.read_parquet(f'data/weather/{file}'))
df.to_parquet('data/all_weather.parquet')
    