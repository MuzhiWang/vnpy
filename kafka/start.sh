#!/bin/bash

#sh ./kafka_2.13-3.0.0/bin/zookeeper-server-start.sh ./kafka_2.13-3.0.0/config/zookeeper.properties &>/dev/null &

sleep 20

sh /kafka/kafka_2.11-2.3.0/bin/kafka-server-start.sh /kafka/kafka_2.11-2.3.0/config/server.properties &>/dev/null &

sleep 10

sh /kafka/kafka_2.11-2.3.0/bin/kafka-topics.sh --create --topic EVENTLOGGG --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1