from time import sleep
from kafka import KafkaAdminClient
from kafka.admin.new_topic import NewTopic
import os

SERVER_ADDRESS = "localhost:9092"


admin_client = KafkaAdminClient(
    bootstrap_servers=SERVER_ADDRESS,
    client_id='admin'
)

topics = admin_client.list_topics()
if "__consumer_offsets" in topics:
    topics.remove("__consumer_offsets")

print("Delete topics:", topics)
admin_client.delete_topics(topics)

