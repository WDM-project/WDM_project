import os
import atexit
import uuid
import requests
from flask import Flask, jsonify
import redis
import json
from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka import TopicPartition
from queue import Queue
from threading import Thread
import time

app = Flask("order-service")

db: redis.Redis = redis.Redis(
    host=os.environ["REDIS_HOST"],
    port=int(os.environ["REDIS_PORT"]),
    password=os.environ["REDIS_PASSWORD"],
    db=int(os.environ["REDIS_DB"]),
)

running_in_kubernetes = os.environ.get("RUNNING_IN_KUBERNETES")

if running_in_kubernetes:
    user_service_url = os.environ["USER_SERVICE_URL"]
    stock_service_url = os.environ["STOCK_SERVICE_URL"]
else:
    gateway_url = os.environ["GATEWAY_URL"]


def close_db_connection():
    db.close()


atexit.register(close_db_connection)


def get_item_price(item_id):
    if running_in_kubernetes:
        response = requests.get(f"{stock_service_url}/find/{item_id}")
    else:
        response = requests.get(f"{gateway_url}/stock/find/{item_id}")

    if response.status_code == 200:
        return response.json()["price"]
    else:
        return None


def subtract_stock_quantity(item_id, quantity):
    if running_in_kubernetes:
        response = requests.post(f"{stock_service_url}/subtract/{item_id}/{quantity}")
    else:
        response = requests.post(f"{gateway_url}/stock/subtract/{item_id}/{quantity}")

    return response.status_code == 200


def add_stock_quantity(item_id, quantity):
    if running_in_kubernetes:
        response = requests.post(f"{stock_service_url}/add/{item_id}/{quantity}")
    else:
        response = requests.post(f"{gateway_url}/stock/add/{item_id}/{quantity}")

    return response.status_code == 200


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
    return response.status_code == 200


@app.post("/create/<user_id>")
def create_order(user_id):
    order_id = str(uuid.uuid4())
    # Making a transaction with pipeline
    pipe = db.pipeline(transaction=True)
    key = "order:" + order_id
    items = []
    try:
        pipe.multi()
        pipe.hset(key, "order_id", order_id)
        pipe.hset(key, "paid", "False")
        pipe.hset(key, "items", json.dumps(items))
        pipe.hset(key, "user_id", user_id)
        pipe.hset(key, "total_cost", 0)
        pipe.execute()
        return jsonify({"order_id": order_id}), 200
    except Exception as e:
        # If an error occurs the transaction will be aborted and no commands will be executed.
        return str(e), 500
    finally:
        # Resetting the pipeline will restore it to its initial state.
        pipe.reset()


@app.delete("/remove/<order_id>")
def remove_order(order_id):
    pipe = db.pipeline(transaction=True)
    try:
        pipe.delete(f"order:{order_id}")
        result = pipe.execute()
        if result[0] == 0:
            return jsonify({"error": "Order not found"}), 400
        return jsonify({"status": "success"}), 200
    except Exception as e:
        return str(e), 500
    finally:
        pipe.reset()


@app.post("/addItem/<order_id>/<item_id>")
def add_item(order_id, item_id):
    order_key = f"order:{order_id}"

    pipe = db.pipeline(transaction=True)
    try:
        pipe.watch(order_key)
        pipe.multi()
        pipe.exists(order_key)
        pipe.hgetall(order_key)
        result = pipe.execute()
        print("add_item, result === ", result)
        # app.logger.debug(f"Pipeline result: {result}")
        if not result:
            return jsonify({"error": "Result Error In Pipe Execution"}), 400
        if not result[0]:
            return jsonify({"error": "The order_key does not exist"}), 400
        order_data = result[1]
        item_price = get_item_price(item_id)
        if item_price is None:
            return jsonify({"error": "Item not found"}), 400
        items = json.loads(order_data[b"items"].decode())
        items.append(item_id)
        total_cost = int(order_data[b"total_cost"]) + item_price
        pipe.multi()
        pipe.hset(order_key, "items", json.dumps(items))
        pipe.hset(order_key, "total_cost", total_cost)
        pipe.execute()
        return jsonify({"status": "success"}), 200
    except Exception as e:
        return str({"error": str(e), "type": str(type(e))}), 500
    finally:
        pipe.unwatch()


@app.delete("/removeItem/<order_id>/<item_id>")
def remove_item(order_id, item_id):
    order_key = f"order:{order_id}"
    print("remove_item, order_key === ", order_key)
    pipe = db.pipeline(transaction=True)
    try:
        pipe.watch(order_key)
        pipe.multi()
        pipe.hgetall(order_key)
        result = pipe.execute()
        print("remove_item, result === ", result)
        order_data = result[0]
        print("remove_item, order_data === ", order_data)
        if not order_data:
            return jsonify({"error": "Order not found"}), 400
        item_price = get_item_price(item_id)
        if item_price is None:
            return jsonify({"error": "Item not found"}), 400
        items = json.loads(order_data[b"items"].decode())
        if item_id not in items:
            return jsonify({"error": "Item not in order"}), 400
        items.remove(item_id)
        total_cost = int(order_data[b"total_cost"]) - item_price
        pipe.multi()
        pipe.hset(order_key, "items", json.dumps(items))
        pipe.hset(order_key, "total_cost", total_cost)
        pipe.execute()
        return jsonify({"status": "success"}), 200
    except Exception as e:
        return str(e), 500
    finally:
        pipe.unwatch()


@app.get("/find/<order_id>")
def find_order(order_id):
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
        order = {
            key.decode(): (
                value.decode() if key != b"items" else json.loads(value.decode())
            )
            for key, value in order_data.items()
        }
        return jsonify(order), 200
    except Exception as e:
        return str(e), 500
    finally:
        pipe.unwatch()


producer = KafkaProducer(
    bootstrap_servers="kafka-service:9092",
    api_version=(0, 11, 5),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


# this function will be executed in a separate thread for each API call
def kafka_consumer_thread(consumer, queue, global_transaction_id):
    for message in consumer:
        print("Received message:", message)
        if message.key == global_transaction_id:
            queue.put(message.value)
            break


@app.post("/checkout/<order_id>")
def checkout(order_id):
    global_transaction_id = str(uuid.uuid4())
    order_key = f"order:{order_id}"
    global_transaction_key = f"global_transaction_id:{global_transaction_id}"
    pipe = db.pipeline(transaction=True)

    try:
        # TODO: see below
        #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        # problem for now, when one instance die, the consumer start from the beginning,
        # but the database does not have the global_transaction_id

        # TODO multiple duplicate calls of the same order_id
        #!!!!!!!!!!!!!!!

        # set up global transaction id
        # pipe.multi()
        # pipe.hset(global_transaction_key, "status", "pending")
        # pipe.hset(global_transaction_key, "order_id", order_id)
        # pipe.execute()

        pipe.watch(order_key)
        pipe.multi()
        pipe.hgetall(order_key)
        result = pipe.execute()
        # order_data = result[0]
        order_data = byte_keys_to_str(result[0])
        if not order_data:
            return jsonify({"error": "Order not found"}), 400
        # if we have order_data:
        # user_id = order_data[b"user_id"].decode()
        # total_cost = int(order_data[b"total_cost"])

        # Start of stock check
        # items = json.loads(order_data[b"items"].decode())
        items = order_data["items"]
        list_data = json.loads(items)
        order_data["items"] = list_data
        # producer.send(
        #     "checkout_topic",
        #     value={"order_data": order_data, "status": "pending"},
        #     key=global_transaction_id,
        # )
        # Start of stock check and payment processing.
        # Send a message to Kafka instead of calling the microservices directly.
        # print("sending stock check message of order_id: ", order_id)
        producer.send(
            "stock_check_topic",
            value={
                "affected_items": list_data,
                "action": "remove",
                "is_roll_back": "false",
            },
            key=global_transaction_id,
            partition=0,
        )
        # print("sending payment processing message of order_id: ", order_id)
        producer.send(
            "payment_processing_topic",
            value={"order_data": order_data, "action": "pay", "is_roll_back": "false"},
            key=global_transaction_id,
            partition=0,
        )

        # for message in consumer:
        #     print("unpack message result in line 312", message)
        #     if message.key == global_transaction_id:
        #         msg = message.value
        #         if msg["status"] == "success":
        #             return jsonify({"status": "success"}), 200
        #         else:
        #             return jsonify({"error": "Payment failed"}), 400
        # return jsonify({"error": "Wait too long, quit"}), 400
        consumer = KafkaConsumer(
            bootstrap_servers="kafka-service:9092",
            api_version=(0, 11, 5),
            group_id=global_transaction_id,
            auto_offset_reset="earliest",
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            key_deserializer=lambda x: json.loads(x.decode("utf-8")),
        )

        consumer.assign([TopicPartition("order_result_topic", 0)])
        print("waiting for order result, consumer has subscribed to order_result_topic")

        queue = Queue()
        consumer_thread = Thread(
            target=kafka_consumer_thread, args=(consumer, queue, global_transaction_id)
        )
        consumer_thread.start()

        # wait for the consumer thread to put a message in the queue
        while True:
            try:
                msg = queue.get(timeout=10)  # wait for 10 seconds
                if msg["status"] == "success":
                    return jsonify({"status": "success"}), 200
                else:
                    return jsonify({"status": "failure"}), 400
            except queue.Empty:
                print("No message received after 10 seconds, retrying...")
                continue

    except Exception as e:
        return str(e), 500
    finally:
        pipe.unwatch()


def byte_keys_to_str(dictionary):
    return {
        k.decode("utf-8"): v.decode("utf-8") if isinstance(v, bytes) else v
        for k, v in dictionary.items()
    }
