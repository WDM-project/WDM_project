import os
import atexit
from flask import Flask, jsonify
import redis
from kafka import KafkaProducer
import json
from kafka import KafkaConsumer
from threading import Thread


app = Flask("payment-service")

db: redis.Redis = redis.Redis(
    host=os.environ["REDIS_HOST"],
    port=int(os.environ["REDIS_PORT"]),
    password=os.environ["REDIS_PASSWORD"],
    db=int(os.environ["REDIS_DB"]),
)


def close_db_connection():
    db.close()


atexit.register(close_db_connection)


@app.post("/create_user")
def create_user():
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


@app.post("/add_funds/<user_id>/<amount>")
def add_credit(user_id: str, amount: int):
    pipe = db.pipeline(transaction=True)
    user_key = f"user:{user_id}"
    try:
        pipe.watch(user_key)
        exists = pipe.exists(user_key)
        if not exists:
            return jsonify({"error": "User not found"}), 400
        pipe.multi()
        pipe.hincrby(user_key, "credit", int(amount))
        pipe.execute()
        return jsonify({"done": True}), 200
    except Exception as e:
        return str(e), 500
    finally:
        pipe.reset()


@app.post("/pay/<user_id>/<order_id>/<amount>")
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
        if current_credit < int(amount):
            return jsonify({"error": "Insufficient credit"}), 400

        pipe.multi()
        pipe.hincrby(user_key, "credit", -int(amount))
        pipe.hset(order_key, "paid", "True")
        pipe.execute()
        return jsonify({"status": "success"}), 200
    except Exception as e:
        return str(e), 500
    finally:
        pipe.reset()


@app.post("/cancel/<user_id>/<order_id>")
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
            return jsonify({"error": "Order not found"}), 400

        if order_data[b"paid"] == b"True":
            total_cost = int(order_data[b"total_cost"])
            pipe.multi()
            pipe.hset(order_key, "paid", "False")
            pipe.hincrby(user_key, "credit", total_cost)
            pipe.execute()
            return jsonify({"status": "success"}), 200
        else:
            return jsonify({"error": "Payment already cancelled"}), 400
    except Exception as e:
        return str(e), 500
    finally:
        pipe.reset()


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
