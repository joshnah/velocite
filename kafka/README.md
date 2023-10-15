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
python3 -m venv sdtd-kafka
source sdtd-kafka/bin/activate
pip install -r requirements.txt # Install dependencies
```

Consumer:

```
source sdtd-kafka/bin/activate
python3 consumer.py
```

Parseur:

```
source sdtd-kafka/bin/activate
python3 parser.py
```

Producer:

```
source sdtd-kafka/bin/activate
python producer.py
```

## References

- https://github.com/obsidiandynamics/kafdrop/tree/master
- https://kafka-python.readthedocs.io/en/master/#
- https://github.com/Tarequzzaman/Kafka-Streaming/tree/main
