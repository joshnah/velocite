import json
import time
from kafka import KafkaProducer
import requests

TOPIC_NAME = "velo"
SERVER_ADDRESS = '127.0.0.1:9092'



producer = KafkaProducer(
    bootstrap_servers=SERVER_ADDRESS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


def calling_api() -> dict:
    # Calling API
    answer = requests.get("https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/records?limit=20").json()
    return answer

if __name__ == "__main__":
    """
       1. Calling API 
       2. Send the fake data to the consumer topic 
       3. Sleep 10 seconds and repeat the process utill press Crontrol C
    """

    try:
        while(True):
            result = calling_api()
            print(result)
            producer.send(TOPIC_NAME, result)
            time.sleep(10)
    except KeyboardInterrupt:
       print("Quit")