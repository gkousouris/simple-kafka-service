# simple-kafka-service
A simple Kafka service that populates data from a JSON into Kafka and allows querying the Kafka topic.

# Assumtions

This assumes a Kafka docker image is already running on 9092 with something like:
```
docker run -p 9092:9092 apache/kafka:3.7.0
```
and that a topic `random_people_data_topic` already exists. To create a topic, you can run:
```
$ docker ps
$ docker exec -it {CONTAINER ID} /bin/bash
$ /opt/kafka/bin/kafka-topics.sh  --create --topic people_data_topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
$ /opt/kafka/bin/kafka-configs.sh --alter --entity-type topics --entity-name people_data_topic --add-config cleanup.policy=delete --bootstrap-server localhost:9092
```

The project uses Maven for dependency management.
