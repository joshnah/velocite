import json
from kafka import KafkaConsumer
import threading
from cassandra.cluster import Cluster

cluster = Cluster(['127.0.0.1'], port=9042)
session = cluster.connect()

SERVER_ADDRESS = '127.0.0.1:9092'
CONSUMER_GROUP_ID = 'group1'
"""
format of the message: {
            [{"capacity":32,
            "lat":44.83803,
            "lon":-0.58437,
            "name":"Meriadeck",
            "station_id":"1",
            num_bikes_available":13,
            last_reported":1697465565
            },...
            ]
    }
"""


consumers = {"paris":"",
             "lille":"",
             "lyon":"",
             "strasbourg":"",
             "toulouse":"",
             "bordeaux":"",
             "nancy":"",
             "amiens":"",
             "besancon":""}
for city in consumers:
    consumers[city] =  KafkaConsumer(
    city,
    bootstrap_servers=SERVER_ADDRESS,
    group_id=CONSUMER_GROUP_ID
)
    
def insert_in_db(city):
    consumer = KafkaConsumer(
        city,
        bootstrap_servers=SERVER_ADDRESS,
        group_id=CONSUMER_GROUP_ID
    )
    try:
        for msg in consumer:
            results = json.loads(msg.value)
            session.execute("USE station")
            for station in results:
                try:
                    session.execute(f"INSERT INTO stations (city, station_id, bikes, capacity, updated_at) VALUES ('{city}', '{station['station_id']}', {station['num_bikes_available']}, {station['capacity']}, {1000*int(station['last_reported'])});")
                except Exception as e:
                    print(e)
                    print(f"Error while inserting {station} in DB")
    except KeyboardInterrupt:
        print(f"{city}: -quit")

if __name__ == "__main__":
    #TODO check real benefices of multithreading in this case
    threads = []
    for city in consumers:
        thread = threading.Thread(target=insert_in_db, args=(city,))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()


