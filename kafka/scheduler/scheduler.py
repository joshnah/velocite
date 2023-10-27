import json
import time
from kafka import KafkaProducer
import requests

import os
from dotenv import load_dotenv

load_dotenv()

TODO_CITIES_TOPIC = os.getenv("TODO_CITIES_TOPIC")
COMPLETE_CITIES_TOPIC = os.getenv("COMPLETE_CITIES_TOPIC")
SERVER_ADDRESS = os.getenv("SERVER_ADDRESS")


producer = KafkaProducer(
    bootstrap_servers=SERVER_ADDRESS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=SERVER_ADDRESS,
    group_id="scheduler"
)

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