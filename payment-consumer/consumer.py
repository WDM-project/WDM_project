from kafka import KafkaConsumer, KafkaProducer, TopicPartition
import json
import os
import redis

# from flask import Flask, jsonify

# app = Flask("payment-consumer-service")

from kafka import KafkaAdminClient
from kafka.admin import NewPartitions


def create_partitions():
    bootstrap_servers = "kafka-service:9092"
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

    topics = [
        "payment_processing_result_topic",
        "stock_check_result_topic",
    ]  # List your topics here
    partition_count = 2  # Number of partitions to be created for each topic

    topic_partitions = {
        topic: NewPartitions(total_count=partition_count) for topic in topics
    }

    try:
        admin_client.create_partitions(topic_partitions)
        print("Partitions created successfully.")
    except Exception as e:
        print(f"Failed to create partitions: {str(e)}")


create_partitions()


db: redis.Redis = redis.Redis(
    host=os.environ["REDIS_HOST"],
    port=int(os.environ["REDIS_PORT"]),
    password=os.environ["REDIS_PASSWORD"],
    db=int(os.environ["REDIS_DB"]),
)

producer = KafkaProducer(
    bootstrap_servers="kafka-service:9092",
    api_version=(0, 11, 5),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

consumer = KafkaConsumer(
    group_id="payment_consumer_group",
    bootstrap_servers="kafka-service:9092",
    api_version=(0, 11, 5),
    auto_offset_reset="earliest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    key_deserializer=lambda x: json.loads(x.decode("utf-8")),
)


def add_credit(user_id: str, amount: int):
    pipe = db.pipeline(transaction=True)
    user_key = f"user:{user_id}"
    try:
        pipe.watch(user_key)
        exists = pipe.exists(user_key)
        if not exists:
            return {"error": "User not found"}, 400
        pipe.multi()
        pipe.hincrby(user_key, "credit", int(amount))
        pipe.execute()
        return {"done": True}, 200
    except Exception as e:
        return str(e), 500
    finally:
        pipe.reset()


def remove_credit(user_id: str, order_id: str, amount: int):
    user_key = f"user:{user_id}"
    order_key = f"order:{order_id}"
    pipe = db.pipeline(transaction=True)
    try:
        pipe.watch(user_key, order_key)
        pipe.multi()
        pipe.hget(user_key, "credit")
        result = pipe.execute()
        current_credit = result[0]
        current_credit = int(current_credit)
        print(
            "current credit",
            current_credit,
            "orderid",
            order_id,
            "in line 44 of payment consumer",
        )
        if current_credit < int(amount):
            return {"error": "Insufficient credit"}, 400

        pipe.multi()
        pipe.hincrby(user_key, "credit", -int(amount))
        pipe.hset(order_key, "paid", "True")
        pipe.execute()
        return {"status": "success"}, 200
    except Exception as e:
        return str(e), 500
    finally:
        pipe.reset()


def cancel_payment(user_id: str, order_id: str):
    user_key = f"user:{user_id}"
    order_key = f"order:{order_id}"
    pipe = db.pipeline(transaction=True)
    try:
        pipe.watch(order_key, user_key)
        pipe.multi()
        pipe.hgetall(order_key)
        result = pipe.execute()
        order_data = result[0]
        if not order_data:
            return {"error": "Order not found"}, 400
        print("order data", order_data)
        if order_data[b"paid"] == b"True":
            total_cost = int(order_data[b"total_cost"])
            pipe.multi()
            pipe.hset(order_key, "paid", "False")
            pipe.hincrby(user_key, "credit", total_cost)
            pipe.execute()
            return {"status": "success"}, 200
        else:
            return {"error": "Payment already cancelled"}, 400
    except Exception as e:
        return str(e), 500
    finally:
        pipe.reset()


consumer.assign([TopicPartition("payment_processing_topic", 0)])
for message in consumer:
    print("Received message in payment consumer")
    msg = message.value
    transaction_id = message.key

    print(msg)
    # if msg["is_roll_back"]=="false" and db.get(f"transaction:{transaction_id}"):
    #     print(f"Transaction {transaction_id} has been processed before, skipping...")
    #     continue
    # # If this is not a rollback operation, store the transaction_id in Redis to mark this operation as processed
    # if msg["is_roll_back"] == "false" and db.get(f"transaction:{transaction_id}") is None:
    #     db.set(f"transaction:{transaction_id}", 1)
    if msg["callFrom"] == "checkout":
        order_data = msg["order_data"]
        order_id = order_data["order_id"]
        user_id = order_data["user_id"]
        total_cost = int(order_data["total_cost"])
        if msg["action"] == "pay":
            print("Going to remove credit")
            response, status_code = remove_credit(user_id, order_id, total_cost)
            print("received response from remove credit", response, status_code)
            if status_code == 200:
                producer.send(
                    "payment_processing_result_topic",
                    key=transaction_id,
                    value={
                        "status": "success",
                        "order_data": order_data,
                        "action": "pay",
                        "is_roll_back": msg["is_roll_back"],
                    },
                    partition=0,
                )
                print("Sent success message to payment processing result topic pay")
            else:
                producer.send(
                    "payment_processing_result_topic",
                    key=transaction_id,
                    value={
                        "status": "failure",
                        "order_data": order_data,
                        "action": "pay",
                        "is_roll_back": msg["is_roll_back"],
                    },
                    partition=0,
                )
                print("Sent failure message to payment processing result topic pay")
        elif msg["action"] == "cancel":
            response, status_code = cancel_payment(user_id, order_id)
            print("received response from cancel payment", response, status_code)
            if status_code == 200:
                producer.send(
                    "payment_processing_result_topic",
                    key=transaction_id,
                    value={
                        "status": "success",
                        "order_data": order_data,
                        "action": "cancel",
                        "is_roll_back": msg["is_roll_back"],
                    },
                    partition=0,
                )
                print("Sent success message to payment processing result topic cancel")
            else:
                producer.send(
                    "payment_processing_result_topic",
                    key=transaction_id,
                    value={
                        "status": "failure",
                        "order_data": order_data,
                        "action": "cancel",
                        "is_roll_back": msg["is_roll_back"],
                    },
                    partition=0,
                )
                print("Sent failure message to payment processing result topic cancel")
    else:
        if msg["action"] == "add_credit":
            user_id = msg["user_id"]
            amount = int(msg["amount"])
            response, status_code = add_credit(user_id, amount)
            print("received response from add credit", response, status_code)
            if status_code == 200:
                producer.send(
                    "payment_processing_result_topic",
                    key=transaction_id,
                    value={
                        "status": "success",
                        "user_id": user_id,
                        "amount": amount,
                        "action": "add_credit",
                        "is_roll_back": msg["is_roll_back"],
                    },
                    partition=1,
                )
                print(
                    "Sent success message to payment processing result topic add_credit"
                )
            else:
                producer.send(
                    "payment_processing_result_topic",
                    key=transaction_id,
                    value={
                        "status": "failure",
                        "user_id": user_id,
                        "amount": amount,
                        "action": "add_credit",
                        "is_roll_back": msg["is_roll_back"],
                    },
                    partition=1,
                )
                print(
                    "Sent failure message to payment processing result topic add_credit"
                )
        elif msg["action"] == "pay":
            user_id = msg["user_id"]
            order_id = msg["order_id"]
            amount = int(msg["amount"])
            response, status_code = remove_credit(user_id, order_id, amount)
            print("received response from remove credit", response, status_code)
            if status_code == 200:
                producer.send(
                    "payment_processing_result_topic",
                    key=transaction_id,
                    value={
                        "status": "success",
                        "user_id": user_id,
                        "order_id": order_id,
                        "amount": amount,
                        "action": "pay",
                        "is_roll_back": msg["is_roll_back"],
                    },
                    partition=1,
                )
                print(
                    "Sent success message to payment processing result topic pay from payment"
                )
            else:
                producer.send(
                    "payment_processing_result_topic",
                    key=transaction_id,
                    value={
                        "status": "failure",
                        "user_id": user_id,
                        "order_id": order_id,
                        "amount": amount,
                        "action": "pay",
                        "is_roll_back": msg["is_roll_back"],
                    },
                    partition=1,
                )
                print(
                    "Sent failure message to payment processing result topic pay from payment"
                )
        elif msg["action"] == "cancel":
            user_id = msg["user_id"]
            order_id = msg["order_id"]
            response, status_code = cancel_payment(user_id, order_id)
            print("received response from cancel payment", response, status_code)
            if status_code == 200:
                producer.send(
                    "payment_processing_result_topic",
                    key=transaction_id,
                    value={
                        "status": "success",
                        "user_id": user_id,
                        "order_id": order_id,
                        "action": "cancel",
                        "is_roll_back": msg["is_roll_back"],
                    },
                    partition=1,
                )
                print(
                    "Sent success message to payment processing result topic cancel from payment"
                )
            else:
                producer.send(
                    "payment_processing_result_topic",
                    key=transaction_id,
                    value={
                        "status": "failure",
                        "user_id": user_id,
                        "order_id": order_id,
                        "action": "cancel",
                        "is_roll_back": msg["is_roll_back"],
                    },
                    partition=1,
                )
                print(
                    "Sent failure message to payment processing result topic cancel from payment"
                )
