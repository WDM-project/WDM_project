import os
import atexit
from flask import Flask, jsonify
import redis

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
        user_id = pipe.incr("user_id")
        pipe.execute()
        user_key = f"user:{user_id}"
        pipe.multi()
        pipe.hset(user_key, {"credit": 0})
        pipe.execute()
        return jsonify({"user_id": user_id}), 200
    except Exception as e:
        return str(e), 400
    

@app.get("/find_user/<user_id>")
def find_user(user_id: str):
    user_key = f"user:{user_id}"
    pipe = db.pipeline(transaction=True)
    try:
        pipe.watch(user_key)
        user_data = pipe.hgetall(user_key)
        pipe.execute()
        if not user_data:
            return jsonify({"error": "User not found"}), 400
        return jsonify({"user_id": int(user_id), "credit": int(user_data[b"credit"])}), 200
    except Exception as e:
        return str(e), 400


@app.post("/add_funds/<user_id>/<amount>")
def add_credit(user_id: str, amount: int):
    pipe = db.pipeline(transaction=True)
    user_key = f"user:{user_id}"
    try:
        pipe.watch(user_key)
        if not db.exists(user_key):
            return jsonify({"error": "User not found"}), 400
        pipe.hincrby(user_key, "credit", int(amount))
        pipe.execute()
        return jsonify({"done": True}), 200
    except Exception as e:
        return str(e), 400


@app.post("/pay/<user_id>/<order_id>/<amount>")
def remove_credit(user_id: str, order_id: str, amount: int):
    user_key = f"user:{user_id}"
    order_key = f"order:{order_id}"
    pipe = db.pipeline(transaction=True)
    try:
        pipe.watch(user_key)
        current_credit = pipe.hget(user_key, "credit")
        pipe.execute()
        current_credit = int(current_credit)
        if current_credit < int(amount):
            return jsonify({"error": "Insufficient credit"}), 400
        
        pipe.multi()
        pipe.hincrby(user_key, "credit", -int(amount))
        pipe.hset(order_key, "paid", "True")
        pipe.execute()
        return jsonify({"status": "success"}), 200
    except Exception as e:
        return str(e), 400


@app.post("/cancel/<user_id>/<order_id>")
def cancel_payment(user_id: str, order_id: str):
    user_key = f"user:{user_id}"
    order_key = f"order:{order_id}"
    pipe = db.pipeline(transaction=True)
    try:
        pipe.watch(order_key)
        order_data = pipe.hgetall(order_key)
        pipe.execute()
        if not order_data:
            return jsonify({"error": "Order not found"}), 400

        if order_data[b"paid"] == b"True":
            pipe.multi()
            pipe.hset(order_key, "paid", "False")
            total_cost = int(order_data[b"total_cost"])
            pipe.hincrby(user_key, "credit", total_cost)
            pipe.execute()
            return jsonify({"status": "success"}), 200
        else:
            return jsonify({"error": "Payment already cancelled"}), 400
    except Exception as e:
        return str(e), 400


@app.get("/status/<user_id>/<order_id>")
def payment_status(user_id: str, order_id: str):
    order_key = f"order:{order_id}"
    pipe = db.pipeline(transaction=True)
    try:
        pipe.watch(order_key)
        order_data = pipe.hgetall(order_key)
        pipe.execute()
        if not order_data:
            return jsonify({"error": "Order not found"}), 400

        paid = True if order_data[b"paid"] == b"True" else False
        return jsonify({"paid": paid}), 200
    except Exception as e:
        return str(e), 400
