import os
import atexit
import uuid
import requests
from flask import Flask, jsonify
import redis
import json
from dtmcli import saga
from dtmcli import utils
from dtmcli import msg

# dtm = "http://localhost:36789/api/dtmsvr"
dtm = "http://dtm:36789/api/dtmsvr"
notification_service_url = "http://localhost:8000/notifications"

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
    pipe = db.pipeline(transaction=True)
    try:
        pipe.watch(order_key)
        pipe.multi()
        pipe.hgetall(order_key)
        result = pipe.execute()
        order_data = result[0]
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

@app.post("/checkout/<order_id>")
def checkout(order_id):
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
        user_id = order_data[b"user_id"].decode()
        total_cost = int(order_data[b"total_cost"])
        items = json.loads(order_data[b"items"].decode())

        # Create a new Saga transaction
        s = saga.Saga(dtm, utils.gen_gid(dtm))

        # Add a step for processing the payment
        s.add(
            {"user_id": user_id, "order_id": order_id, "total_cost": total_cost},
            f"{gateway_url}/payment/pay/{user_id}/{order_id}/{total_cost}",
            f"{gateway_url}/payment/add_funds/{user_id}/{total_cost}"
        )

        # Add a step for each item in the order
        # for item_id in items:
        #     s.add(
        #         {"item_id": item_id, "quantity": 1},
        #         f"{gateway_url}/stock/subtract/{item_id}/1",
        #         f"{gateway_url}/stock/add/{item_id}/1"
        #     )

        # Submit the Saga transaction
        s.submit()

        # Create a new Msg transaction for notifying the user
        # m = msg.Msg(dtm, utils.gen_gid(dtm))

        # # Add a step for sending an order confirmation email
        # m.add(
        #     {"user_id": user_id, "order_id": order_id},
        #     f"{notification_service_url}/send_order_confirmation/{user_id}/{order_id}"
        # )

        # # Submit the Msg transaction
        # m.submit()

        # Mark the order as paid
        pipe.multi()
        pipe.hset(order_key, "paid", "True")
        pipe.execute()
        return jsonify({"status": "success"}), 200
    except Exception as e:
        return str(e), 500
    finally:
        pipe.unwatch()


