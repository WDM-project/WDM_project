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


def process_payment(user_id, order_id, total_cost):
    if running_in_kubernetes:
        response = requests.post(
            f"{user_service_url}/pay/{user_id}/{order_id}/{total_cost}"
        )
    else:
        response = requests.post(
            f"{gateway_url}/payment/pay/{user_id}/{order_id}/{total_cost}"
        )
    return response


def cancel_payment(user_id, order_id):
    if running_in_kubernetes:
        response = requests.post(f"{user_service_url}/cancel/{user_id}/{order_id}")
    else:
        response = requests.post(f"{gateway_url}/payment/cancel/{user_id}/{order_id}")
    return response


# Setup Kafka consumer
consumer = KafkaConsumer(
    "payment_processing_topic",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="payment-consumer-group",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)

# Setup Kafka consumer
consumer2 = KafkaConsumer(
    "payment_rollback_topic",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="payment-consumer-group",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)


# Setup Kafka producer
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def consumer_payment(consumer):
    for message in consumer:
        msg = message.value
        order_data = msg["order_data"]
        order_id = order_data[b"order_id"].decode()
        user_id = order_data[b"user_id"].decode()
        total_cost = int(order_data[b"total_cost"])
        transaction_id = message.key

        producer.send(
            "payment_processing_service_topic",
            key=transaction_id,
            value={
                "order_id": order_id,
                "user_id": user_id,
                "total_cost": total_cost,
            },
        )
        # Call the function to check and update the stock.
        success = process_payment(
            user_id=user_id, order_id=order_id, total_cost=total_cost
        )

        if not success.status_code == 200:
            producer.send(
                "payment_processing_result_topic",
                {"transaction_id": transaction_id, "status": "failure"},
            )
        else:
            producer.send(
                "payment_processing_result_topic",
                {"transaction_id": transaction_id, "status": "success"},
            )


def consumer_payment_rollback(consumer2):
    for message in consumer2:
        msg = message.value
        order_data = msg["order_data"]
        order_id = order_data[b"order_id"].decode()
        user_id = order_data[b"user_id"].decode()
        total_cost = int(order_data[b"total_cost"])
        transaction_id = message.key

        producer.send(
            "payment_rollback_service_topic",
            key=transaction_id,
            value={
                "order_id": order_id,
                "user_id": user_id,
                "total_cost": total_cost,
            },
        )
        # Call the function to check and update the stock.
        success = cancel_payment(user_id=user_id, order_id=order_id)

        if not success.status_code == 200:
            producer.send(
                "payment_rollback_result_topic",
                {"transaction_id": transaction_id, "status": "failure"},
            )
        else:
            producer.send(
                "payment_rollback_result_topic",
                {"transaction_id": transaction_id, "status": "success"},
            )


# Start consumer threads
consumer_thread1 = Thread(target=consumer_payment, args=(consumer,))
consumer_thread2 = Thread(target=consumer_payment_rollback, atgs=(consumer2,))

consumer_thread1.start()
consumer_thread2.start()
