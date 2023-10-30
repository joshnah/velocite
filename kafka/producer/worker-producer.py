import logging

from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.coordinator.assignors import roundrobin
from kq import Worker
import kq
import os
import json
from dotenv import load_dotenv

load_dotenv()

SERVER_ADDRESS = os.getenv("SERVER_ADDRESS")
RESULT_TOPIC = os.getenv("RESULT_TOPIC")
COMPLETED_PARTITIONS_TOPIC = os.getenv("COMPLETED_PARTITIONS_TOPIC")

# # Set up logging.
formatter = logging.Formatter('[%(levelname)s]')
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger = logging.getLogger('kq.worker')
logger.setLevel(logging.DEBUG)
logger.addHandler(stream_handler)

# Set up a Kafka consumer.
consumer = KafkaConsumer(
    bootstrap_servers='127.0.0.1:9092',
    group_id='group',
    partition_assignment_strategy=[roundrobin.RoundRobinPartitionAssignor],
)

producer = KafkaProducer(
    bootstrap_servers=SERVER_ADDRESS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Callback function to be called by the worker when the job is processed
def callback(status, message, job, result, exception, stacktrace):
    assert isinstance(message, kq.Message)
    city = job.args[0]
    finished = False
    if status == 'success':
        print('Partitions assigned:',consumer.assignment())
        finished = True
        producer.send(RESULT_TOPIC, value=result)
        producer.flush()
        print("worker finished in partition", message.partition)
        # todo: acknowledge the message?
        # send the free partition to the scheduler

    else: #case invalid, timeout or failure
        print("worker failed or timeout in partition", message.partition,"\n")
        if status == 'failure':
            print(exception)
        

    producer.send(COMPLETED_PARTITIONS_TOPIC, value={'city': city, 'finished': finished, 'partition': message.partition})
    producer.flush()

partitions = consumer.partitions_for_topic('to-do-cities')
print('All partitions:',partitions)
print('Partitions assigned:',consumer.assignment())
# consumer.assign([TopicPartition('to-do-cities', 0)])  
# print(consumer.position(TopicPartition('to-do-cities', 0)))


# Set up a worker.
worker = Worker(topic='to-do-cities', consumer=consumer, callback=callback)
worker.start()