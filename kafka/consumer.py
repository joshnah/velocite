import json
from kafka import KafkaConsumer
import threading


SERVER_ADDRESS = '127.0.0.1:9092'
CONSUMER_GROUP_ID = 'group1'


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
    #TODO really insert in cassandradb
    consumer = KafkaConsumer(
        city,
        bootstrap_servers=SERVER_ADDRESS,
        group_id=CONSUMER_GROUP_ID
    )
    try:
        for msg in consumer:
            print(f"{city}: {json.loads(msg.value)}")
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


