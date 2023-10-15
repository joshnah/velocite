import json
import time
from kafka import KafkaProducer
import requests

TOPIC_NAME = "raw_data"
SERVER_ADDRESS = '127.0.0.1:9092'

def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(
    bootstrap_servers=SERVER_ADDRESS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer


def calling_api() -> dict:
    # Calling API
    answer = requests.get("https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/records?limit=20")
    return answer.json()

if __name__ == "__main__":
    """
       1. Calling API 
       2. Send the fake data to the consumer topic 
       3. Sleep 10 seconds and repeat the process utill press Crontrol C
    """
    producer = connect_kafka_producer()
    try:
        while(True):
            result = calling_api()
            print(result)
            producer.send(TOPIC_NAME, result)
            time.sleep(5)
    except KeyboardInterrupt:
       print("Quit")