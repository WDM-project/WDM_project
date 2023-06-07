import os

# from flask import Flask, jsonify
import redis
import json
from kafka import KafkaProducer
from kafka import KafkaConsumer, TopicPartition
import threading

# app = Flask("order-consumer-service")

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
    group_id="order-consumer-group",
    bootstrap_servers="kafka-service:9092",
    api_version=(0, 11, 5),
    auto_offset_reset="earliest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    key_deserializer=lambda x: json.loads(x.decode("utf-8")),
)


class state_tracker:
    def __init__(self):
        self.lock = threading.Lock()
        self.state = {}
        self.stock_check_result = {}
        self.payment_processing_result = {}


consumer.assign(
    [
        TopicPartition("stock_check_result_topic", 0),
        TopicPartition("payment_processing_result_topic", 0),
    ]
)
print(
    "subscribed to stock_check_result_topic and payment_processing_result_topic in order-consumer"
)
state = state_tracker()


def process_message(message):
    print("message received at order-consumer and message is:", message)
    msg = message.value
    transaction_id = message.key
    # if msg["is_roll_back"]=="false" and db.get(f"transaction:{transaction_id}"):
    #     print(f"Transaction {transaction_id} has been processed before, skipping...")
    #     return
    # # If this is not a rollback operation, store the transaction_id in Redis to mark this operation as processed
    # if msg["is_roll_back"] == "false" and db.get(f"transaction:{transaction_id}") is None:
    #     db.set(f"transaction:{transaction_id}", 1)

    if msg["is_roll_back"] == "true":
        # in case of rollback failure, keep trying to rollback
        print("rollback message received at order-consumer")
        if msg["status"] == "failure":
            print("Oops, rollback failed,retrying... for message:", message)
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
                        partition=0,
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
                        partition=0,
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
                        partition=0,
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
                        partition=0,
                    )
        # rollback succeed, no action needed
        else:
            print("rollback succeeded for message:", message)
        # continue
    # normal message, not a rollback
    else:
        if message.topic == "stock_check_result_topic":
            print("stock_check_result_topic received at order-consumer")
            with state.lock:
                state.stock_check_result[transaction_id] = message
            # state.stock_check_result[transaction_id] = message
            # check if the transaction_id is present in both the state variables
            if transaction_id in state.payment_processing_result:
                print("both the results are present under if")
                print("the current transaction id is ", transaction_id)
                stock_check_result = msg.get("status")
                payment_processing_result = state.payment_processing_result.get(
                    transaction_id
                ).value.get("status")
                print(
                    "payment_processing_result is:",
                    payment_processing_result,
                    "and stock_check_result is:",
                    stock_check_result,
                )
                if (
                    payment_processing_result == "success"
                    and stock_check_result == "success"
                ):
                    print(
                        "both the results are success, sending out the order_result_topic message"
                    )
                    producer.send(
                        "order_result_topic",
                        key=transaction_id,
                        value={"status": "success", "reason": "order placed"},
                        partition=0,
                    )
                elif (
                    payment_processing_result == "failure"
                    and stock_check_result == "failure"
                ):
                    print(
                        "both the results are failure, sending out the order_result_topic message"
                    )
                    producer.send(
                        "order_result_topic",
                        key=transaction_id,
                        value={
                            "status": "failure",
                            "reason": "payment and stock both fails",
                        },
                        partition=0,
                    )
                elif (
                    payment_processing_result == "success"
                    and stock_check_result == "failure"
                ):
                    print(
                        "payment_processing_result is success and stock_check_result is failure, sending out the order_result_topic message"
                    )
                    producer.send(
                        "order_result_topic",
                        key=transaction_id,
                        value={"status": "failure", "reason": "stock check fails"},
                        partition=0,
                    )
                    # send the rollback message to the payment_processing_topic
                    temp_msg = state.payment_processing_result.get(transaction_id).value
                    if temp_msg["action"] == "pay":
                        print(
                            "sending rollback message to payment_processing_topic with cancel"
                        )
                        producer.send(
                            "payment_processing_topic",
                            key=transaction_id,
                            value={
                                "order_data": temp_msg["order_data"],
                                "action": "cancel",
                                "is_roll_back": "true",
                            },
                            partition=0,
                        )
                    elif temp_msg["action"] == "cancel":
                        print(
                            "sending rollback message to payment_processing_topic with pay"
                        )
                        producer.send(
                            "payment_processing_topic",
                            key=transaction_id,
                            value={
                                "order_data": temp_msg["order_data"],
                                "action": "pay",
                                "is_roll_back": "true",
                            },
                            partition=0,
                        )
                elif (
                    payment_processing_result == "failure"
                    and stock_check_result == "success"
                ):
                    print(
                        "payment_processing_result is failure and stock_check_result is success, sending out the order_result_topic message"
                    )
                    producer.send(
                        "order_result_topic",
                        key=transaction_id,
                        value={"status": "failure", "reason": "payment fails"},
                        partition=0,
                    )
                    # send the rollback message to the stock_check_topic
                    temp_msg = state.stock_check_result.get(transaction_id).value
                    if temp_msg["action"] == "add":
                        print(
                            "sending rollback message to stock_check_topic with remove action in line 191"
                        )
                        producer.send(
                            "stock_check_topic",
                            key=transaction_id,
                            value={
                                "affected_items": temp_msg["affected_items"],
                                "action": "remove",
                                "is_roll_back": "true",
                            },
                            partition=0,
                        )
                    elif temp_msg["action"] == "remove":
                        print(
                            "sending rollback message to stock_check_topic with add in line 204"
                        )
                        producer.send(
                            "stock_check_topic",
                            key=transaction_id,
                            value={
                                "affected_items": temp_msg["affected_items"],
                                "action": "add",
                                "is_roll_back": "true",
                            },
                            partition=0,
                        )
            else:
                print("only stock_check_result is present")
        elif message.topic == "payment_processing_result_topic":
            print("payment_processing_result_topic received at order-consumer")
            with state.lock:
                state.payment_processing_result[transaction_id] = message
            # state.payment_processing_result[transaction_id] = message
            # check if the transaction_id is present in both the state variables
            if transaction_id in state.stock_check_result:
                print("both the results are present under elif")
                print("the current transaction id is ", transaction_id)
                stock_check_result = state.stock_check_result.get(
                    transaction_id
                ).value.get("status")
                payment_processing_result = msg.get("status")
                print(
                    "stock_check_result is :",
                    stock_check_result,
                    "payment_processing_result is :",
                    payment_processing_result,
                )
                if (
                    payment_processing_result == "success"
                    and stock_check_result == "success"
                ):
                    print(
                        "both success, going to send success order result topic message"
                    )
                    producer.send(
                        "order_result_topic",
                        key=transaction_id,
                        value={"status": "success", "reason": "both success"},
                        partition=0,
                    )
                elif (
                    payment_processing_result == "failure"
                    and stock_check_result == "failure"
                ):
                    print(
                        "both failure, going to send failure order result topic message"
                    )
                    producer.send(
                        "order_result_topic",
                        key=transaction_id,
                        value={"status": "failure", "reason": "both fails"},
                        partition=0,
                    )
                elif (
                    payment_processing_result == "success"
                    and stock_check_result == "failure"
                ):
                    print(
                        "payment success and stock failure, going to send failure order result topic message"
                    )
                    producer.send(
                        "order_result_topic",
                        key=transaction_id,
                        value={"status": "failure", "reason": "stock check fails"},
                        partition=0,
                    )
                    # send the rollback message to the payment_processing_topic
                    temp_msg = state.payment_processing_result.get(transaction_id).value
                    if temp_msg["action"] == "pay":
                        print(
                            "rollback enabled in line 263, rolling back the payment with cancel action"
                        )
                        producer.send(
                            "payment_processing_topic",
                            key=transaction_id,
                            value={
                                "order_data": temp_msg["order_data"],
                                "action": "cancel",
                                "is_roll_back": "true",
                            },
                            partition=0,
                        )
                    elif temp_msg["action"] == "cancel":
                        print(
                            "rollback enabled in line 273, rolling back the payment with pay action"
                        )
                        producer.send(
                            "payment_processing_topic",
                            key=transaction_id,
                            value={
                                "order_data": temp_msg["order_data"],
                                "action": "pay",
                                "is_roll_back": "true",
                            },
                            partition=0,
                        )
                elif (
                    payment_processing_result == "failure"
                    and stock_check_result == "success"
                ):
                    print(
                        "payment failure and stock success, going to send failure order result topic message"
                    )
                    producer.send(
                        "order_result_topic",
                        key=transaction_id,
                        value={"status": "failure", "reason": "payment fails"},
                        partition=0,
                    )
                    # send the rollback message to the stock_check_topic
                    temp_msg = state.stock_check_result.get(transaction_id).value
                    if temp_msg["action"] == "add":
                        print(
                            "rollback enabled in line 296, rolling back the stock check with remove action"
                        )
                        producer.send(
                            "stock_check_topic",
                            key=transaction_id,
                            value={
                                "affected_items": temp_msg["affected_items"],
                                "action": "remove",
                                "is_roll_back": "true",
                            },
                            partition=0,
                        )
                    elif temp_msg["action"] == "remove":
                        print(
                            "rollback enabled in line 311, rolling back the stock check with add action"
                        )
                        producer.send(
                            "stock_check_topic",
                            key=transaction_id,
                            value={
                                "affected_items": temp_msg["affected_items"],
                                "action": "add",
                                "is_roll_back": "true",
                            },
                            partition=0,
                        )
            else:
                print("only payment_processing_result is present")

        else:
            raise Exception("Invalid topic from order processing")


for message in consumer:
    threading.Thread(target=process_message, args=(message,)).start()
