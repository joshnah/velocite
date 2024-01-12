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
        INSERT INTO stations (city, station_id, updated_at, bikes, capacity)
        VALUES (%(city)s, %(station_id)s, %(updated_at)s, %(bikes)s, %(capacity)s)
    """
    session.execute(query, data)

def insert_mock_data(session, station_ids, start_date, end_date):
    current_date = start_date
    while current_date <= end_date:
        for station_id in station_ids:
            mock_data = {
                "city": "lyon",
                "station_id": station_id,
                "updated_at": current_date,
                "bikes": random.randint(0, 20),
                "capacity": 20  # You can adjust these values as needed
            }
            insert_data(session, mock_data)

        current_date += timedelta(hours=1)

if __name__ == "__main__":
    session = create_session()

    file_path = "station_id.txt"
    start_date = datetime.now() - timedelta(days=3)
    end_date = datetime.now() - timedelta(days=1)

    station_ids = get_station_list(url)
    insert_mock_data(session, station_ids, start_date, end_date)
