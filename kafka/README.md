# Projet SDTD

Docker compose for setting up Kafka (with zookeeper) and Kafdrop

```
docker-compose up
```

Background mode (daemon):

```
docker-compose up -d
```


Create a virtual-environment & activate it

```
python -m venv sdtd-kafka
source sdtd-kafka/bin/activate
pip install -r requirements.txt # Install dependencies
```

#### Dont forget to rename the file .env.example to .env

#### For each terminal, you need to activate the virtual environment

### Admin: 
Delete all topics and recreate the "to-do-cities" topic with configurable number of workers (default: 2)

```
python admin/admin.py
```


### Consumer:

```
python consumer/consumer.py
```

### Worker-producer
On local, you need to run the each worker-producer in a separate terminal

```
python producer/worker_producer.py
```

After launching all workers, wait for 5s and then run the scheduler in another terminal to schedule the tasks

### Scheduler for producer
```
python producer/scheduler.py
```

### UI kafka with kowl

```
docker run --network=host -p 8080:8080 -e KAFKA_BROKERS=localhost:9092 quay.io/cloudhut/kowl:master 
```

## References

- https://github.com/obsidiandynamics/kafdrop/tree/master
- https://kafka-python.readthedocs.io/en/master/#
- https://github.com/Tarequzzaman/Kafka-Streaming/tree/main
