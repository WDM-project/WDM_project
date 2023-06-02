from kafka import KafkaConsumer, KafkaProducer
import json
import requests
import os
from threading import Thread

running_in_kubernetes = os.environ.get("RUNNING_IN_KUBERNETES")

if running_in_kubernetes:
    user_service_url = os.environ["USER_SERVICE_URL"]
    stock_service_url = os.environ["STOCK_SERVICE_URL"]
else:
    gateway_url = os.environ["GATEWAY_URL"]


def subtract_stock_quantity(item_id, quantity):
    if running_in_kubernetes:
        response = requests.post(f"{stock_service_url}/subtract/{item_id}/{quantity}")
    else:
        response = requests.post(f"{gateway_url}/stock/subtract/{item_id}/{quantity}")

    return response


def add_stock_quantity(item_id, quantity):
    if running_in_kubernetes:
        response = requests.post(f"{stock_service_url}/add/{item_id}/{quantity}")
    else:
        response = requests.post(f"{gateway_url}/stock/add/{item_id}/{quantity}")

    return response


# Setup Kafka consumer
consumer = KafkaConsumer(
    "stock_check_topic",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="stock-consumer-group",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)

consumer2 = KafkaConsumer(
    "stock_rollback_topic",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="stock-consumer-group",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)

# Setup Kafka producer
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def consumer_stock(consumer):
    for message in consumer:
        msg = message.value
        transaction_id = message.key
        order_id = msg["order_id"]
        order_data = msg["order_data"]
        items = json.loads(order_data[b"items"].decode())

        affected_items = []

        for item_id, quantity in items.items():
            success = subtract_stock_quantity(item_id, quantity)
            affected_items.append(item_id)
            if not success.status_code == 200:
                # If the stock check/update fails, produce a message to the result topic with status 'failure'.
                producer.send(
                    "stock_check_result_topic",
                    {
                        "transaction_id": transaction_id,
                        "status": "failure",
                        "affected_items": affected_items,
                    },
                )

        # If the stock check/update succeeds, produce a message to the result topic with status 'success'.
        producer.send(
            "stock_check_result_topic",
            {
                "transaction_id": transaction_id,
                "status": "success",
                "affected_items": affected_items,
            },
        )


def consumer_stock_rollback(consumer2):
    for message in consumer2:
        msg = message.value
        transaction_id = message.key
        order_id = msg["order_id"]
        order_data = msg["order_data"]
        items = json.loads(order_data[b"items"].decode())
        affected_items = []

        for item_id, quantity in items.items():
            success = add_stock_quantity(item_id, quantity)
            affected_items.append(item_id)
            if not success.status_code == 200:
                # If the stock check/update fails, produce a message to the result topic with status 'failure'.
                producer.send(
                    "stock_check_result_rollback_topic",
                    {"transaction_id": transaction_id, "status": "failure"},
                )
                还要写失败rollback怎么办
        else:
            # If the stock check/update succeeds, produce a message to the result topic with status 'success'.
            producer.send(
                "stock_check_result_rollback_topic",
                {"transaction_id": transaction_id, "status": "success"},
            )


# Start consumer threads
consumer_thread1 = Thread(target=consumer_stock, args=(consumer,))
consumer_thread2 = Thread(target=consumer_stock_rollback, atgs=(consumer2,))

consumer_thread1.start()
consumer_thread2.start()
