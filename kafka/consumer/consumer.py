import json
from kafka import KafkaConsumer
import os
from dotenv import load_dotenv
from cassandra.cluster import Cluster


load_dotenv()

RESULT_TOPIC = os.getenv("RESULT_TOPIC")
SERVER_ADDRESS = os.getenv("SERVER_ADDRESS")
CONSUMER_GROUP_ID = os.getenv("CONSUMER_GROUP_ID")
CASSANDRA_ADDRESS = os.getenv("CASSANDRA_ADDRESS")
CASSANDRA_PORT = os.getenv("CASSANDRA_PORT")

cluster = Cluster([CASSANDRA_ADDRESS], port=CASSANDRA_PORT)
session = cluster.connect()

# defining consumer
consumer = KafkaConsumer(
    RESULT_TOPIC,
    bootstrap_servers=SERVER_ADDRESS,
    group_id=CONSUMER_GROUP_ID
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

