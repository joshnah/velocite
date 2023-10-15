import json
from kafka import KafkaProducer
from kafka import KafkaConsumer


TOPIC_NAME = "raw_data"
SERVER_ADDRESS = '127.0.0.1:9092'

def parse(raw_data):
    # Do some data treatment here
    j = json.loads(raw_data.decode('utf8'))
    return j

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

if __name__ == '__main__':
    print('Running Parseur')
    raw_topic_name = 'raw_data'
    parsed_topic_name = 'parsed_data'
    producer = connect_kafka_producer()
    parser_consumer = KafkaConsumer(raw_topic_name, bootstrap_servers=SERVER_ADDRESS)

    for msg in parser_consumer:
        result = parse(msg.value)
        print("parsed data ", result)
        producer.send('parsed_data', result)

    parser_consumer.close()