import dateutil.parser as parser
import sqlite3
import json
import datetime
import requests
import urllib2
import pandas as pd

from bs4 import BeautifulSoup


# Weather Api -> Forecast data
def GetForecast():
    url = 'http://api2.climacell.co/v2'
    forecast_header = {'apikey': 'hPtFGmXimTSHzgcQcgyQ0pEKG5uWriRG'}
    forecast_payload = {'lat': '40.7831', 'lon': '73.9712', 'num_hours': '24'}

    ForecastResponse = requests.get('http://api2.climacell.co/v2/weather/forecast/hourly', params=forecast_payload,
                                    headers=forecast_header)
    ForecastData = ForecastResponse.json()
    return ForecastData


# Weather Api -> Historical Data
def GetHistorical():

    endtime = datetime.datetime(2018, 7, 1, 0, 0, 0)
    histdata = []

    for weeks in range(1, 3):
        starttime = endtime - datetime.timedelta(days=6)
        endtimestring = endtime.isoformat()
        starttimestring = starttime.isoformat()

        histurl = "https://api2.climacell.co/v2/historical"
        histpayload = json.dumps({
            "geocode": {
                "lon": -71.17609710203855,
                "lat": 42.30260171891152},
            "location_id": "",
            "start_time": starttimestring,
            "end_time": endtimestring,
            "timestep": 1440,
            "fields": [
                {
                    "name": "feels_like",
                    "units": "C"
                },
                {
                    "name": "precipitation_type"
                },
                {
                    "name": "precipitation",
                    "units": "mm/hr"
                },
                {
                    "name": "wind_speed",
                    "units": "kph"
                },
                {
                    "name": "visibility",
                    "units": "km"
                },
                {
                    "name": "baro_pressure",
                    "units": "mmHg"
                }]})
        histheaders = {
            'apikey': "hPtFGmXimTSHzgcQcgyQ0pEKG5uWriRG",
            'accept': "application/json",
            'Content-Type': "application/json",
            'Cache-Control': "no-cache",
            'Postman-Token': "22f0c3ce-1597-43c2-b9c1-33c524339570"
        }

        histresponse = requests.request("POST", histurl, data=histpayload, headers=histheaders)
        histdata.extend(histresponse.json())
        endtime = endtime - datetime.timedelta(days=7)
    return histdata


# From API data -> pandas array
def IntoArray(ApiData):
    A = []
    B = []
    C = []
    D = []
    E = []
    F = []
    G = []

    for observation in ApiData:
        A.append(observation['observation_time']['value'])
        B.append(observation['feels_like']['value'])
        C.append(observation['precipitation_type']['value'])
        D.append(observation['precipitation']['value'])
        E.append(observation['wind_speed']['value'])
        F.append(observation['visibility']['value'])
        G.append(observation['baro_pressure']['value'])

    df = pd.DataFrame(A, columns=['Timestamp'])
    df['feels_like'] = B
    df['precipitation_type'] = C
    df['precipitation'] = D
    df['wind_speed'] = E
    df['visibility'] = F
    df['baro_pressure'] = G

    return df


# Forecast weather data frame -> sqlite DB
def Forecastsql(Forecastdf):
    conn = sqlite3.connect("fore.db")
    Forecastdf.to_sql('ForecastDB', conn, if_exists='replace')
    c = conn.cursor()
    c.execute("SELECT * FROM ForecastDB")
    print(c.fetchall())


# Historical weather data frame into sqlite DB
def Histsql(Histdf):
    conn = sqlite3.connect("hist.db")
    Histdf.to_sql('HistDB', conn, if_exists='replace')
    c = conn.cursor()
    c.execute("SELECT * FROM HistDB")
    print(c.fetchall())


# scraper: from scraper to df
def GetCrypto():
    coinurl = "https://coinmarketcap.com/currencies/bitcoin/historical-data/?start=20170801&end=20180801"
    page = urllib2.urlopen(coinurl)
    soup = BeautifulSoup(page)

    A = []
    B = []
    C = []
    D = []
    E = []
    F = []
    G = []

    table = soup.find('table', class_='table')
    for row in table.findAll('tr'):
        cells = row.findAll ('td')
        if len(cells) == 7:
            date = parser.parse(cells[0].find(text=True))

            A.append(date.isoformat())
            B.append(cells[1].find(text=True))
            C.append(cells[2].find(text=True))
            D.append(cells[3].find(text=True))
            E.append(cells[4].find(text=True))
            F.append(cells[5].find(text=True))
            G.append(cells[6].find(text=True))


    df = pd.DataFrame(A, columns=['Timestamp'])

    df['Open'] = B
    df['High'] = C
    df['Low'] = D
    df['Close'] = E
    df['Volume'] = F
    df['Market_Cap'] = G

    return df


# scraper: from df to sqlite
def Cryptosql(df):
    conn = sqlite3.connect("crypto.db")
    df.to_sql('CryptoDB', conn, if_exists='replace')
    c = conn.cursor()

    print(pd.read_sql_query("select * from CryptoDB;", conn))


# Main
histdata = GetHistorical()
Histdf = IntoArray(histdata)
Histsql(Histdf)

forecastdata = GetForecast()
forecastdf = IntoArray(forecastdata)
Forecastsql(forecastdf)

cryptodf = GetCrypto()
Cryptosql(cryptodf)

