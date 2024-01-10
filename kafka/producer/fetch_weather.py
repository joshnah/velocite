import requests

from datetime import datetime
from cassandra.cluster import Cluster



cluster = Cluster(['cassandra-service.database.svc.cluster.local'], port=9042)
session = cluster.connect()

list_apis = {"paris": "75056",
           "lille": "59350",
           "lyon": "13055",
           "strasbourg": "67482",
           "toulouse": "31555",
           "bordeaux": "33063",
           "nancy": "54395",
           "amiens": "80021",
           "besancon": "25056"}


def extract_from_weather_api(city):
    url = "https://api.meteo-concept.com/api/forecast/nextHours?token=070e9a04c5915656119e3f60f3f5e0f3825b4c113d9f30abc55d5796376b7b17&insee=" + \
        list_apis[city]+"&hourly=true"
    try:
        # add header with User-Agent Accept Json
        headers = {'User-Agent': 'Mozilla/5.0',
                     'Accept': 'application/json'}
        weather_request = requests.get(url, headers=headers, timeout=5).json()
    except Exception as e:
        print(e)
        print("Error while calling API informations from meteo")
        return None
    forecasts = weather_request["forecast"]
    return forecasts
    

if __name__ == "__main__":
    """
        1. Extract forecasts from api
        2. Insert or update into db
    """

    # truncate table
    session.execute("TRUNCATE station.weather")
    forecasts_all = []
    for city in list_apis.keys():
        forecasts = extract_from_weather_api(city)
        if forecasts:
            forecasts_all.append(forecasts)

    for forecasts in forecasts_all:
        for forecast in forecasts:
            city = [k for k, v in list_apis.items() if v == forecast["insee"]][0]
            proba_rain = forecast["probarain"]
            forecast_date = datetime.strptime(forecast["datetime"], '%Y-%m-%dT%H:%M:%S%z')
            prepared_query = session.prepare(
                "INSERT INTO station.weather (city, proba_rain, forecast_at) VALUES (?, ?, ?)")
            session.execute(prepared_query, (city, proba_rain, forecast_date))
            print("Inserted forecast for city {} at {}".format(city, forecast_date))
    print("Done")
    
