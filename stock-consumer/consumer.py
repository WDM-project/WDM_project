from kafka import KafkaConsumer, KafkaProducer
import json
import requests
import os
from flask import Flask, jsonify
import redis

app = Flask("stock-consumer-service")


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
    bootstrap_servers="kafka:9092",
    auto_offset_reset="earliest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    key_deserializer=lambda x: json.loads(x.decode("utf-8")),
)


def modify_stock_list(items: list, amount: int):
    pipe = db.pipeline(transaction=True)
    # First phase: check all items
    for item_id in items:
        item_key = f"item:{item_id}"
        stock = db.hget(item_key, "stock")
        # + not - because sign of amount indicates the direction of the transaction
        if stock is None or int(stock) + amount < 0:
            return (
                jsonify(
                    {
                        "error": "Insufficient stock or item not found for item_id: "
                        + str(item_id)
                    }
                ),
                400,
            )

    # Second phase: actual decrement
    try:
        for item_id in items:
            item_key = f"item:{item_id}"
            pipe.hincrby(item_key, "stock", amount)
        pipe.execute()
        return jsonify({"done": True}), 200
    except Exception as e:
        return str(e), 500
    finally:
        pipe.reset()


consumer.subscribe(["stock_check_topic"])
for message in consumer:
    msg = msg.value
    transaction_id = message.key
    affected_items = msg["affected_items"]
    # reverse_items = []
    if msg["action"] == "add":
        response = modify_stock_list(affected_items, 1)
        if response.status_code != 200:
            producer.send(
                "stock_check_result_topic",
                key=transaction_id,
                value={
                    "status": "failture",
                    "affected_items": affected_items,
                    "is_roll_back": msg["is_roll_back"],
                    "action": "add",
                },
            )
        else:
            producer.send(
                "stock_check_result_topic",
                key=transaction_id,
                value={
                    "status": "success",
                    "affected_items": affected_items,
                    "is_roll_back": msg["is_roll_back"],
                    "action": "add",
                },
            )
        # reverse_items.append(item_id)
    elif msg["action"] == "remove":
        response = modify_stock_list(affected_items, 1)
        if response.status_code != 200:
            producer.send(
                "stock_check_result_topic",
                key=transaction_id,
                value={
                    "status": "failture",
                    "affected_items": affected_items,
                    "is_roll_back": msg["is_roll_back"],
                    "action": "remove",
                },
            )
        else:
            producer.send(
                "stock_check_result_topic",
                key=transaction_id,
                value={
                    "status": "success",
                    "affected_items": affected_items,
                    "is_roll_back": msg["is_roll_back"],
                    "action": "remove",
                },
            )
        # reverse_items.append(item_id)
