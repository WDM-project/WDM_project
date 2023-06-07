import os
import atexit
from flask import Flask, jsonify
import redis
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
import json
import uuid


app = Flask("payment-service")

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
    group_id="payment-group",
    bootstrap_servers="kafka-service:9092",
    api_version=(0, 11, 5),
    auto_offset_reset="earliest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    key_deserializer=lambda x: json.loads(x.decode("utf-8")),
)


def close_db_connection():
    db.close()


atexit.register(close_db_connection)


@app.post("/create_user")
def create_user_init():
    pipe = db.pipeline(transaction=True)
    try:
        pipe.incr("user_id")
        result = pipe.execute()  # The result of the INCR command is stored in `result`
        user_id = result[
            0
        ]  # The result of the INCR command is the first element of `result`
        user_key = f"user:{user_id}"
        pipe.multi()
        pipe.hset(user_key, "credit", 0)
        pipe.execute()
        return jsonify({"user_id": user_id}), 200
    except Exception as e:
        return str(e), 500
    finally:
        pipe.reset()


@app.post("/create_user/<user_id>")
def create_user(user_id):
    pipe = db.pipeline(transaction=True)
    try:
        # pipe.incr("user_id")
        # result = pipe.execute()  # The result of the INCR command is stored in `result`
        # user_id = result[
        # 0
        # ]  # The result of the INCR command is the first element of `result`
        user_key = f"user:{user_id}"
        pipe.multi()
        pipe.hset(user_key, "credit", 0)
        pipe.execute()
        return jsonify({"user_id": user_id}), 200
    except Exception as e:
        return str(e), 500
    finally:
        pipe.reset()


@app.get("/find_user/<user_id>")
def find_user(user_id: str):
    user_key = f"user:{user_id}"
    pipe = db.pipeline(transaction=True)
    try:
        pipe.watch(user_key)
        pipe.multi()
        pipe.hgetall(user_key)
        result = pipe.execute()
        user_data = result[0]
        if not user_data:
            return jsonify({"error": "User not found"}), 400
        return (
            jsonify({"user_id": int(user_id), "credit": int(user_data[b"credit"])}),
            200,
        )
    except Exception as e:
        return str(e), 500
    finally:
        pipe.reset()


consumer.assign([TopicPartition("payment_processing_result_topic", 1)])


@app.post("/add_funds/<user_id>/<amount>")
def add_credit(user_id: str, amount: int):
    ida = str(uuid.uuid4())
    producer.send(
        "payment_processing_topic",
        value={
            "user_id": user_id,
            "amount": amount,
            "action": "add_credit",
            "is_roll_back": "false",
            "callFrom": "payment",
        },
        key=ida,
        partition=0,
    )
    for message in consumer:
        msg = message.value
        if message.key == ida:
            if msg["status"] == "success":
                return jsonify({"status": "success"}), 200
            else:
                return jsonify({"status": "failed"}), 400


@app.post("/pay/<user_id>/<order_id>/<amount>")
def remove_credit(user_id: str, order_id: str, amount: int):
    idr = str(uuid.uuid4())
    producer.send(
        "payment_processing_topic",
        key=idr,
        value={
            "user_id": user_id,
            "amount": amount,
            "action": "pay",
            "is_roll_back": "false",
            "callFrom": "payment",
            "order_id": order_id,
        },
        partition=0,
    )
    for message in consumer:
        msg = message.value
        if message.key == idr:
            if msg["status"] == "success":
                return jsonify({"status": "success"}), 200
            else:
                return jsonify({"status": "failed"}), 400


@app.post("/cancel/<user_id>/<order_id>")
def cancel_payment(user_id: str, order_id: str):
    idc = str(uuid.uuid4())
    producer.send(
        "payment_processing_topic",
        key=idc,
        value={
            "user_id": user_id,
            "action": "cancel",
            "is_roll_back": "false",
            "callFrom": "payment",
            "order_id": order_id,
        },
        partition=0,
    )
    for message in consumer:
        msg = message.value
        if message.key == idc:
            if msg["status"] == "success":
                return jsonify({"status": "success"}), 200
            else:
                return jsonify({"status": "failed"}), 400


@app.get("/status/<user_id>/<order_id>")
def payment_status(user_id: str, order_id: str):
    order_key = f"order:{order_id}"
    pipe = db.pipeline(transaction=True)
    try:
        pipe.watch(order_key)
        pipe.multi()
        pipe.hgetall(order_key)
        result = pipe.execute()
        order_data = result[0]
        if not order_data:
            return jsonify({"error": "Order not found"}), 400

        paid = True if order_data[b"paid"] == b"True" else False
        return jsonify({"paid": paid}), 200
    except Exception as e:
        return str(e), 500
    finally:
        pipe.reset()
