from cassandra.cluster import Cluster
from datetime import datetime, timedelta
import requests
import random


# get the list of station from the api
url = "https://transport.data.gouv.fr/gbfs/lyon/station_information.json"


def get_station_list(url):
    response = requests.get(url)
    return [station["station_id"] for station in  response.json()["data"]["stations"]]

def create_session():
    cluster = Cluster(['localhost'])
    session = cluster.connect('station')  # Replace 'your_keyspace' with your actual keyspace
    return session

def insert_data(session, data):
    query = """
        INSERT INTO stations (city, station_id, updated_at, bikes, capacity, proba_rain)
        VALUES (%(city)s, %(station_id)s, %(updated_at)s, %(bikes)s, %(capacity)s, %(proba_rain)s)
    """
    session.execute(query, data)

def insert_weather(session, current_date):
    proba_rain = random.randint(0, 100)
    query_select = """
        SELECT DISTINCT city, station_id FROM station.stations;
    """
    result_set = session.execute(query_select)
    query = """
        INSERT INTO stations (city, station_id, updated_at, proba_rain) 
        VALUES (%(city)s, %(station_id)s, %(updated_at)s, %(proba_rain)s)
    """
    # pour chaque couple city, station_id on insère une proba de pluie
    for row in result_set:
        mock_data = {
            "city": row.city,
            "station_id": row.station_id,
            "updated_at": current_date,
            "proba_rain": proba_rain
        }
        session.execute(query, mock_data)
    
    

def insert_mock_data(session, station_ids, start_date, end_date):
    current_date = start_date
    while current_date <= end_date:
        for station_id in station_ids:
            random_bikes = random.randint(0, 20)
            mock_data = {
                "city": "lyon",
                "station_id": station_id,
                "updated_at": current_date,
                "bikes": random_bikes,
                "capacity": 20,  # You can adjust these values as needed
                "proba_rain": random.randint(0, 100)
            }
            insert_data(session, mock_data)
        current_date += timedelta(hours=1)
    while current_date <= end_date + timedelta(hours=12):
        insert_weather(session, current_date)
        current_date += timedelta(hours=1)
        

if __name__ == "__main__":
    session = create_session()
    # flush the table
    session.execute("TRUNCATE stations")
    start_date = datetime(2023, 12, 15, 0, 0, 0)
    end_date = datetime.now()

    station_ids = get_station_list(url)
    insert_mock_data(session, station_ids, start_date, end_date)