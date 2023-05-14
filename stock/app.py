import os
import atexit
from flask import Flask, jsonify
import redis

app = Flask("stock-service")

db: redis.Redis = redis.Redis(
    host=os.environ["REDIS_HOST"],
    port=int(os.environ["REDIS_PORT"]),
    password=os.environ["REDIS_PASSWORD"],
    db=int(os.environ["REDIS_DB"]),
)


def close_db_connection():
    db.close()


atexit.register(close_db_connection)


@app.post("/item/create/<price>")
def create_item(price: int):
    pipe = db.pipeline(transaction=True)
    try:
        pipe.watch("item_id")
        item_id = pipe.incr("item_id")
        pipe.execute()
        item_key = f"item:{item_id}"
        pipe.multi()
        pipe.hset(item_key, {"price": price, "stock": 0})
        pipe.execute()
        return jsonify({"item_id": item_id}), 200
    except Exception as e:
        return str(e), 200


@app.get("/find/<item_id>")
def find_item(item_id: str):
    item_key = f"item:{item_id}"
    pipe = db.pipeline(transaction=True)
    try:
        pipe.watch(item_key)
        item_data = pipe.hgetall(item_key)
        pipe.execute()
        if not item_data:
            return jsonify({"error": "Item not found"}), 400
        return (
            jsonify({"stock": int(item_data[b"stock"]), "price": int(item_data[b"price"])}),
            200,
        )
    except Exception as e:
        return str(e), 200


@app.post("/add/<item_id>/<amount>")
def add_stock(item_id: str, amount: int):
    item_key = f"item:{item_id}"
    pipe = db.pipeline(transaction=True)
    try:
        pipe.watch(item_key)
        is_exist = pipe.exists(item_key)
        pipe.execute()
        if not is_exist:
            return jsonify({"error": "Item not found"}), 400
        pipe.multi()
        pipe.hincrby(item_key, "stock", int(amount))
        pipe.execute()
        return jsonify({"done": True}), 200
    except Exception as e:
        return str(e), 200


@app.post("/subtract/<item_id>/<amount>")
def remove_stock(item_id: str, amount: int):
    item_key = f"item:{item_id}"
    pipe = db.pipeline(transaction=True)
    try:
        pipe.watch(item_key)
        is_exist = pipe.exists(item_key)
        pipe.execute()
        if not is_exist:
            return jsonify({"error": "Item not found"}), 400

        
        current_stock = pipe.hget(item_key, "stock")
        pipe.execute()
        current_stock = int(current_stock)
        
        if current_stock < int(amount):
            return jsonify({"error": "Insufficient stock"}), 400

        pipe.hincrby(item_key, "stock", -int(amount))
        pipe.execute()
        return jsonify({"done": True}), 200
    except Exception as e:
        return str(e), 200
