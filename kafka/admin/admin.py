from time import sleep
from kafka import KafkaAdminClient
from kafka.admin.new_topic import NewTopic
from os.path import join, dirname
from dotenv import load_dotenv
import os

dotenv_path = join(dirname(__file__), '../.env')
load_dotenv()

NB_WORKER = int(os.getenv("NB_WORKERS"))
SERVER_ADDRESS = os.getenv("SERVER_ADDRESS")


admin_client = KafkaAdminClient(
    bootstrap_servers=SERVER_ADDRESS,
    client_id='admin'
)

topics = admin_client.list_topics()
if "__consumer_offsets" in topics:
    topics.remove("__consumer_offsets")

print("Delete topics:", topics)
admin_client.delete_topics(topics)

sleep(1)

print("Creating topics...")
admin_client.create_topics([
    NewTopic(name="to-do-cities", num_partitions=NB_WORKER, replication_factor=1),
])

print("New topics:", admin_client.list_topics())