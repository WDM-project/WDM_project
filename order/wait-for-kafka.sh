#!/bin/sh

echo "Waiting for Kafka to be available..."

until nc -z kafka-service 9092; do
  sleep 5
done

echo "Kafka is up - executing command"
exec $@

