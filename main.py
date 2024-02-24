import requests
import sqlite3
import time
import pandas as pd
from bokeh.plotting import figure, show
from bokeh.models import ColumnDataSource


conn = sqlite3.connect('weather.db')
cursor = conn.cursor()


def last_executed():

        try:
                with open('last_executed.txt') as f:
                        last_execution_date = int(f.read())
        except FileNotFoundError:
                last_execution_date = int(time.time())

        return last_execution_date


def update_executed():
    with open('last_executed.txt', 'w') as f:
        f.write(str(int(time.time())))


def get_links():
    
    last_execution_date = last_executed()

    if last_execution_date // 86400 ==  time.time() // 86400:
        return []

    links = []

    def build_link(date):
        lat = '51.5072'
        lon = '0.1276'
        from key import key
        link = f'https://api.openweathermap.org/data/3.0/onecall/timemachine?lat={lat}&lon={lon}&dt={date}&appid={key}&units=metric'
        links.append(link)

    nearest_hour = int(time.time() - time.time() % 3600)
    nearest_hour_old = last_execution_date - last_execution_date % 3600

    for x in range(nearest_hour_old+3600, nearest_hour, 3600):
        build_link(x)
    
    return links


def clean_data(data):

    d1 = {
         'lat': data['lat'],
         'lon': data['lon'],
         'timezone': data['timezone'],
         'timezone_offset': data['timezone_offset']
          }

    for i in data['data']:
        d1.update(i)

    for i in d1['weather']:
        d1.update(i)

    d1.pop('weather')

    data = d1

    if 'rain' in data.keys():
        data['rain'] = [*data['rain'].values()][0]

    if 'snow' in data.keys():
        data['snow'] = [*data['snow'].values()][0]

    all_api_keys = [
         'lat', 'lon', 'timezone', 'timezone_offset', 'dt', 
         'sunrise', 'sunset', 'temp', 'feels_like', 'pressure', 
         'humidity', 'dew_point', 'uvi', 'clouds', 'visibility', 
         'wind_speed', 'wind_deg', 'wind_gust', 'id', 'main', 
         'description', 'icon', 'rain', 'snow'
                    ]
    
    for i in all_api_keys:
        if i not in data.keys():
            i = {i: 'N/A'}
            data.update(i)

    return data


def extract_load(links):

    cursor.executescript('''

            CREATE TABLE IF NOT EXISTS weather (
                     lat,
                     lon,
                     timezone,
                     timezone_offset,
                     date_time,
                     sunrise,
                     sunset,
                     temp,
                     feels_like,
                     pressure,
                     humidity,
                     dew_point,
                     uvi,
                     clouds,
                     visibility,
                     wind_speed,
                     wind_gust,
                     wind_deg,
                     weather_id,
                     main,
                     description,
                     icon,
                     rain,
                     snow     
            )
        ''')

    for link in links:

        response = requests.get(link)

        data = clean_data(response.json())

        cursor.execute('''INSERT INTO weather VALUES (:lat, :lon, :timezone, :timezone_offset,
                       :dt, :sunrise, :sunset, :temp, :feels_like, :pressure, :humidity, 
                       :dew_point, :uvi, :clouds, :visibility, :wind_speed, :wind_gust, :wind_deg, 
                       :id, :main, :description, :icon, :rain, :snow)''', data
                       )
        
        conn.commit()


def plot_temp_feels_like(data):

    source = ColumnDataSource(data)

    p = figure(x_axis_label=r'\[\text{ Temperature }^\circ C\]',
               y_axis_label=r'\[\text{ Feels like temp }^\circ C\]')

    p.circle(x='temp',
             y='feels_like', source=source, fill_color='blue')

    show(p)


def plot_temp_pressure(data):

    source = ColumnDataSource(data)

    p = figure(x_axis_label=r'\[\text{ Temperature }^\circ C\]',
               y_axis_label=r'\[\text{ Pressure }hPa\]')

    p.circle(x='temp',
             y='pressure', source=source, fill_color='blue')

    show(p)


def plot_wind_s_d(data):
 
    source = ColumnDataSource(data)

    p = figure(x_axis_label=r'\[\text{ Wind Direction }\]',
                y_axis_label=r'\[\text{ Wind Speed }ms^{-1}\]')

    p.circle(x='wind_deg',
            y='wind_speed', source=source, fill_color='blue')

    show(p)

update_executed()
if __name__ == '__main__':
     extract_load(get_links())
     df = pd.read_sql_query('''SELECT * FROM weather''', conn)
     plot_temp_feels_like(df)
     plot_temp_pressure(df)
     plot_wind_s_d(df)
     update_executed()