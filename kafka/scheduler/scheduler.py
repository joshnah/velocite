from kafka import KafkaProducer,KafkaConsumer
import os
import kq
import helper
import json
import schedule
import time
from dotenv import load_dotenv
load_dotenv()

TODO_CITIES_TOPIC = os.getenv("TODO_CITIES_TOPIC")
SERVER_ADDRESS = os.getenv("SERVER_ADDRESS")
COMPLETED_PARTITIONS_TOPIC = os.getenv("COMPLETED_PARTITIONS_TOPIC")
MAX_RETRIES = 3
TIMEOUT = 5
FETCH_INTERVAL = 5

producer = KafkaProducer(
    bootstrap_servers=SERVER_ADDRESS,
)

consumer = KafkaConsumer(
    COMPLETED_PARTITIONS_TOPIC,
    bootstrap_servers=SERVER_ADDRESS,
    group_id="scheduler",
    enable_auto_commit=False,
    # Heartbeats are used to ensure that the consumer’s session stays active
    # As this is a single consumer group, we can increase the heartbeat interval. Otherwise kafka will reblance the group
    heartbeat_interval_ms=10000,
    # The session timeout is used to detect failures. Heartbeats interval should be lower than session timeout/3
    session_timeout_ms=30000,
)


list_apis = {"paris": "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/records",
           "lille": "https://opendata.lillemetropole.fr/api/explore/v2.1/catalog/datasets/vlille-realtime/records",
           "lyon": "https://transport.data.gouv.fr/gbfs/lyon/station_information.json",
           "strasbourg": "https://data.strasbourg.eu/api/explore/v2.1/catalog/datasets/stations-velhop/records",
           "toulouse": "https://data.toulouse-metropole.fr/api/explore/v2.1/catalog/datasets/api-velo-toulouse-temps-reel/records",
           "bordeaux": "https://transport.data.gouv.fr/gbfs/vcub/station_information.json",
           "nancy": "https://transport.data.gouv.fr/gbfs/nancy/station_information.json",
           "amiens": "https://transport.data.gouv.fr/gbfs/amiens/station_information.json",
           "besancon": "https://transport.data.gouv.fr/gbfs/besancon/station_information.json"}
 

# scheduler queue for api calls
queue = kq.Queue(topic=TODO_CITIES_TOPIC, producer=producer)

# Number of partitions of the topic
nb_partitions = len(producer.partitions_for(TODO_CITIES_TOPIC))


def schedule_new_city(partition, list_city_to_process, processing):
    city = list_city_to_process.pop()
    queue.using(partition=partition, key=None,timeout=TIMEOUT).enqueue(helper.extract_from_api, city)
    processing[city] = 0
    producer.flush()
    print(f"adding {city} to processing\n")



def job():
    
    print("#### SCHEDULER IS RUNNING ####\n")
    start_time = time.time()

    # list of cities to process
    list_city_to_process = list(list_apis.keys())

    # Dictionary of cities being processed as keys and number of retries as values. Ex: {"paris": 2}
    processing = {}
    # First, schedule "nb_partitions" firsts cities

    print("Scheduling first cities\n")
    for i in range(nb_partitions):
        schedule_new_city(i, list_city_to_process, processing)

    # for the rest
    print(f"list_city_to_process {list_city_to_process}")
    print(f"processing {processing}")
    print("\nScheduling the rest\n")


    msg_count = 0
    for completed in consumer:
        msg_count += 1
        print(f"\nMESSAGE {msg_count}\n")

        # read the message
        value = json.loads(completed.value)
        print("value",value)
        city = value['city']
        partition = value['partition']
        finished = value['finished']
        print(f"city {city} is {'finished' if {finished} else 'not finished'} by partition {partition}")
        print(f"list_city_to_process {list_city_to_process}")
        print(f"processing {processing}")
        consumer.commit()
        if finished == True and city in processing:
            # if the city is finished, we remove it from the processing list
            print(f"removing {city} from processing\n")
            processing.pop(city)
            print(f"list_city_to_process {list_city_to_process}")
            print(f"processing {processing}\n")

            if len(list_city_to_process) > 0:
                schedule_new_city(partition, list_city_to_process, processing)
            elif len(processing) == 0: # No more cities to process 
                break

        elif city in processing: # the city should be in processing 

            if processing[city] < MAX_RETRIES:
                queue.using(partition=partition, key=None,timeout=TIMEOUT).enqueue(helper.extract_from_api, city)
                processing[city] += 1
                print(f"Retrying {city} for the {processing[city]}th time in partition {partition} \n")
            else:
                print(f"removing {city} from processing due to max retries \n")
                processing.pop(city)
                if len(list_city_to_process) > 0:
                    schedule_new_city(partition, list_city_to_process, processing)
                elif len(processing) == 0: # No more cities to process 
                    break
        # print(f"list_city_to_process {list_city_to_process}")
        # print(f"processing {processing}")
    print("#### SCHEDULER IS DONE WITHIN %s SECONDS ####\n" % (time.time() - start_time))



if __name__ == "__main__":
    # Execute job() every FETCH_INTERVAL seconds
    schedule.every(FETCH_INTERVAL).seconds.do(job)
    while True:
        schedule.run_pending()
        time.sleep(1)

consumer.close()