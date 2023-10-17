import json
from kafka import KafkaConsumer
import os
from dotenv import load_dotenv

load_dotenv()

TOPIC_NAME = os.getenv("TOPIC_NAME")
SERVER_ADDRESS = os.getenv("SERVER_ADDRESS")
CONSUMER_GROUP_ID = os.getenv("CONSUMER_GROUP_ID")

#defining consumer 
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=SERVER_ADDRESS,
    group_id=CONSUMER_GROUP_ID
)

if __name__ == "__main__":
    """Consume Data from the consumer """

    try:
        for msg in consumer:
            print (json.loads(msg.value))
    except KeyboardInterrupt:
        print("-quit")


