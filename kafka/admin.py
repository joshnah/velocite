from kafka import KafkaAdminClient
from kafka.admin.new_partitions import NewPartitions
from kafka.admin.new_topic import NewTopic

admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092",
    client_id='test'
)

# admin_client.delete_topics(['to-do-cities'])
admin_client.create_topics([
    NewTopic(name="to-do-cities", num_partitions=2, replication_factor=1),
])
# List all topics.

# print(admin_client.list_topics())
# admin_client.create_partitions({
#     'to-do-cities': NewPartitions(2),
# })