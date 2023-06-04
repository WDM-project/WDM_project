#!/bin/sh

echo "Waiting for Kafka to be available..."

until nc -z kafka 9092; do
  sleep 5
done

echo "Kafka is up - executing command"
exec $@

RUN chmod +x /wait-for-kafka.sh