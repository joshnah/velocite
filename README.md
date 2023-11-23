# Projet SDTD

## Start services
To start the services, run the following command:
```bash
docker-compose up
```
This will start the following services:
- Cassandra
- Kafka

## Webserver
The webserver is under the `webserver` directory. It is a FastAPI webserver that exposes a REST API to expose the data on Cassandra. For more information, see the [README](webserver/README.md) in the `webserver` directory.

## Kafka
The Kafka service is under the `kafka` directory. For more information, see the [README](kafka/README.md) in the `kafka` directory.
