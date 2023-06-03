import os
import atexit
import uuid
import requests
from flask import Flask, jsonify
import redis
import json
from kafka import KafkaProducer
from kafka import KafkaConsumer
from threading import Thread
import asyncio

app = Flask("order-consumer-service")

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
    auto_offset_reset="latest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    key_deserializer=lambda x: json.loads(x.decode("utf-8")),
)


class state_tracker:
    def __init__(self):
        self.state = {}
        self.stock_check_result = {}
        self.payment_processing_result = {}


consumer.subscribe(["stock_check_result_topic", "payment_processing_result_topic"])
state = state_tracker()

for message in consumer:
    msg = message.value
    transaction_id = message.key
    if msg["is_roll_back"] == "true":
        # in case of rollback failure, keep trying to rollback
        if msg["status"] == "failure":
            if message.topic == "stock_check_result_topic":
                if msg["action"] == "add":
                    producer.send(
                        "stock_check_topic",
                        key=transaction_id,
                        value={
                            "affected_items": msg["affected_items"],
                            "action": "add",
                            "is_roll_back": "true",
                        },
                    )
                elif msg["action"] == "remove":
                    producer.send(
                        "stock_check_topic",
                        key=transaction_id,
                        value={
                            "affected_items": msg["affected_items"],
                            "action": "remove",
                            "is_roll_back": "true",
                        },
                    )
            elif message.topic == "payment_processing_result_topic":
                if msg["action"] == "pay":
                    producer.send(
                        "payment_processing_topic",
                        key=transaction_id,
                        value={
                            "order_data": msg["order_data"],
                            "action": "pay",
                            "is_roll_back": "true",
                        },
                    )
                elif msg["action"] == "cancel":
                    producer.send(
                        "payment_processing_topic",
                        key=transaction_id,
                        value={
                            "order_data": msg["order_data"],
                            "action": "cancel",
                            "is_roll_back": "true",
                        },
                    )
        # rollback succeed, no action needed
        else:
            continue
    # normal message, not a rollback
    if message.topic == "stock_check_result_topic":
        state.stock_check_result[transaction_id] = message
        # check if the transaction_id is present in both the state variables
        if transaction_id in state.payment_processing_result:
            stock_check_result = msg.get("status")
            payment_processing_result = state.payment_processing_result.get(
                transaction_id
            ).value.get("status")
            if (
                payment_processing_result == "success"
                and stock_check_result == "success"
            ):
                producer.send(
                    "order_result_topic",
                    key=transaction_id,
                    value={"status": "success"},
                )
            elif (
                payment_processing_result == "failure"
                or stock_check_result == "failure"
            ):
                producer.send(
                    "order_result_topic",
                    key=transaction_id,
                    value={"status": "failure"},
                )
            elif (
                payment_processing_result == "success"
                and stock_check_result == "failure"
            ):
                producer.send(
                    "order_result_topic",
                    key=transaction_id,
                    value={"status": "failure"},
                )
                # send the rollback message to the payment_processing_topic
                temp_msg = state.payment_processing_result.get(transaction_id).value
                if temp_msg["action"] == "pay":
                    producer.send(
                        "payment_processing_topic",
                        key=transaction_id,
                        value={
                            "order_data": temp_msg["order_data"],
                            "action": "cancel",
                            "is_roll_back": "true",
                        },
                    )
                elif temp_msg["action"] == "cancel":
                    producer.send(
                        "payment_processing_topic",
                        key=transaction_id,
                        value={
                            "order_data": temp_msg["order_data"],
                            "action": "pay",
                            "is_roll_back": "true",
                        },
                    )
            elif (
                payment_processing_result == "failure"
                and stock_check_result == "success"
            ):
                producer.send(
                    "order_result_topic",
                    key=transaction_id,
                    value={"status": "failure"},
                )
                # send the rollback message to the stock_check_topic
                temp_msg = state.stock_check_result.get(transaction_id).value
                if temp_msg["action"] == "add":
                    producer.send(
                        "stock_check_topic",
                        key=transaction_id,
                        value={
                            "affected_items": temp_msg["affected_items"],
                            "action": "remove",
                            "is_roll_back": "true",
                        },
                    )
                elif temp_msg["action"] == "remove":
                    producer.send(
                        "stock_check_topic",
                        key=transaction_id,
                        value={
                            "affected_items": temp_msg["affected_items"],
                            "action": "add",
                            "is_roll_back": "true",
                        },
                    )
        else:
            continue
    elif message.topic == "payment_processing_result_topic":
        state.payment_processing_result[transaction_id] = message
        # check if the transaction_id is present in both the state variables
        if transaction_id in state.stock_check_result:
            stock_check_result = state.stock_check_result.get(transaction_id).value.get(
                "status"
            )
            payment_processing_result = msg.get("status")
            if (
                payment_processing_result == "success"
                and stock_check_result == "success"
            ):
                producer.send(
                    "order_result_topic",
                    key=transaction_id,
                    value={"status": "success"},
                )
            elif (
                payment_processing_result == "failure"
                or stock_check_result == "failure"
            ):
                producer.send(
                    "order_result_topic",
                    key=transaction_id,
                    value={"status": "failure"},
                )
            elif (
                payment_processing_result == "success"
                and stock_check_result == "failure"
            ):
                producer.send(
                    "order_result_topic",
                    key=transaction_id,
                    value={"status": "failure"},
                )
                # send the rollback message to the payment_processing_topic
                temp_msg = state.payment_processing_result.get(transaction_id).value
                if temp_msg["action"] == "pay":
                    producer.send(
                        "payment_processing_topic",
                        key=transaction_id,
                        value={
                            "order_data": temp_msg["order_data"],
                            "action": "cancel",
                            "is_roll_back": "true",
                        },
                    )
                elif temp_msg["action"] == "cancel":
                    producer.send(
                        "payment_processing_topic",
                        key=transaction_id,
                        value={
                            "order_data": temp_msg["order_data"],
                            "action": "pay",
                            "is_roll_back": "true",
                        },
                    )
            elif (
                payment_processing_result == "failure"
                and stock_check_result == "success"
            ):
                producer.send(
                    "order_result_topic",
                    key=transaction_id,
                    value={"status": "failure"},
                )
                # send the rollback message to the stock_check_topic
                temp_msg = state.stock_check_result.get(transaction_id).value
                if temp_msg["action"] == "add":
                    producer.send(
                        "stock_check_topic",
                        key=transaction_id,
                        value={
                            "affected_items": temp_msg["affected_items"],
                            "action": "remove",
                            "is_roll_back": "true",
                        },
                    )
                elif temp_msg["action"] == "remove":
                    producer.send(
                        "stock_check_topic",
                        key=transaction_id,
                        value={
                            "affected_items": temp_msg["affected_items"],
                            "action": "add",
                            "is_roll_back": "true",
                        },
                    )
        else:
            continue
    else:
        raise Exception("Invalid topic from order processing")
