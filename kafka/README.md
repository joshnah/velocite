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
python -m venv env
source env/bin/activate
```

Consumer:

```
python consumer.py
```

In another terminal, set again the environment

```
source env/bin/activate
```

Producer:

```
python consumer.py
```

Kafdrop link: `localhost:9000`

## References

- https://github.com/obsidiandynamics/kafdrop/tree/master
- https://kafka-python.readthedocs.io/en/master/#
- https://github.com/Tarequzzaman/Kafka-Streaming/tree/main
