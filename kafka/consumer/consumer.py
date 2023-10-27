import json
from kafka import KafkaConsumer
import os
from dotenv import load_dotenv
from cassandra.cluster import Cluster


cluster = Cluster(['127.0.0.1'], port=9042)
session = cluster.connect()


load_dotenv()

RESULT_TOPIC = os.getenv("RESULT_TOPIC")
SERVER_ADDRESS = os.getenv("SERVER_ADDRESS")

# defining consumer
consumer = KafkaConsumer(
    RESULT_TOPIC,
    bootstrap_servers=SERVER_ADDRESS,
    group_id='consumer'
)


def insert_in_db(result):
    session.execute("USE station")
    for station in result['stations']:
        try:
            session.execute(
                f"INSERT INTO stations (city, station_id, bikes, capacity, updated_at) VALUES ('{result['city']}', '{station['station_id']}', {station['num_bikes_available']}, {station['capacity']}, {1000*int(station['last_reported'])});")
        except Exception as e:
            print(e)
            print(f"Error while inserting {station} in DB")


if __name__ == "__main__":
    try:
        for result in consumer:
            result = json.loads(result.value)
            city = result['city']
            print(f"Inserting {city} in DB")
            insert_in_db(result)

    except KeyboardInterrupt:
        print("-quit")

