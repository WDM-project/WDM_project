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
        pipe.incr("item_id")
        item_id = pipe.execute()[0]  # Get the results of the pipeline
        item_key = f"item:{item_id}"
        pipe.hset(item_key, "price", price)
        pipe.hset(item_key, "stock", 0)
        pipe.execute()  # Execute the remaining operations
        return jsonify({"item_id": item_id}), 200
    except Exception as e:
        return str(e), 500
    finally:
        pipe.reset()


@app.get("/find/<item_id>")
def find_item(item_id: str):
    item_key = f"item:{item_id}"
    # pipe = db.pipeline(transaction=True)
    try:
        # pipe.watch(item_key)
        item_data = db.hgetall(item_key)
        if not item_data:
            return jsonify({"error": "Item not found"}), 400
        return (
            jsonify(
                {"stock": int(item_data[b"stock"]), "price": int(item_data[b"price"])}
            ),
            200,
        )
    except Exception as e:
        return str(e), 500


@app.post("/add/<item_id>/<amount>")
def add_stock(item_id: str, amount: int):
    item_key = f"item:{item_id}"
    pipe = db.pipeline(transaction=True)
    try:
        pipe.exists(item_key)
        pipe.hincrby(item_key, "stock", int(amount))
        result = pipe.execute()
        if not result[0]:  # check the result of the EXISTS command
            return jsonify({"error": "Item not found"}), 400
        return jsonify({"done": True}), 200
    except Exception as e:
        return str(e), 500
    finally:
        pipe.reset()


@app.post("/subtract/<item_id>/<amount>")
def remove_stock(item_id: str, amount: int):
    item_key = f"item:{item_id}"
    pipe = db.pipeline(transaction=True)
    try:
        pipe.exists(item_key)
        pipe.hget(item_key, "stock")
        result = pipe.execute()
        if not result[0]:  # check the result of the EXISTS command
            return jsonify({"error": "Item not found"}), 400
        current_stock = int(result[1])  # get the current stock
        if current_stock < int(amount):
            return jsonify({"error": "Insufficient stock"}), 400
        db.hincrby(item_key, "stock", -int(amount))  # subtract from the stock
        return jsonify({"done": True}), 200
    except Exception as e:
        return str(e), 500
    finally:
        pipe.reset()
