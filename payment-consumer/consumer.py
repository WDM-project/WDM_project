from kafka import KafkaConsumer, KafkaProducer
import json
import os
import redis
import threading

# from flask import Flask, jsonify

# app = Flask("payment-consumer-service")

db_lock = threading.Lock()

db: redis.Redis = redis.Redis(
    host=os.environ["REDIS_HOST"],
    port=int(os.environ["REDIS_PORT"]),
    password=os.environ["REDIS_PASSWORD"],
    db=int(os.environ["REDIS_DB"]),
)

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

consumer = KafkaConsumer(
    group_id="payment_consumer_group",
    bootstrap_servers="kafka:9092",
    auto_offset_reset="earliest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    key_deserializer=lambda x: json.loads(x.decode("utf-8")),
)


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


consumer.subscribe(["payment_processing_topic"])


def handle_message(message):
    print("Received message in payment consumer")
    msg = message.value
    transaction_id = message.key
    order_data = msg["order_data"]
    order_id = order_data["order_id"]
    user_id = order_data["user_id"]
    total_cost = int(order_data["total_cost"])
    print(message)

    if msg["action"] == "pay":
        print("Going to remove credit")
        with db_lock:
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
            )
            print("Sent failure message to payment processing result topic pay")
    elif msg["action"] == "cancel":
        with db_lock:
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
            )
            print("Sent failure message to payment processing result topic cancel")


for message in consumer:
    thread = threading.Thread(target=handle_message, args=(message,))
    thread.start()
