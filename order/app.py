import os
import atexit
import uuid
import requests
from flask import Flask, jsonify
import redis
import json

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


# # Publish an event
# def publish_event(event_type, data):
#     event = {'type': event_type, 'data': data}
#     db.publish('events', json.dumps(event))


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

# @app.post("/checkout/<order_id>")
# def checkout(order_id):
#     order_key = f"order:{order_id}"
#     pipe = db.pipeline(transaction=True)
#     try:
#         pipe.watch(order_key)
#         pipe.multi()
#         pipe.hgetall(order_key)
#         result = pipe.execute()
#         order_data = result[0]
#         if not order_data:
#             return jsonify({"error": "Order not found"}), 400
        
#         # if we have order_data:
#         user_id = order_data[b"user_id"].decode()
#         total_cost = int(order_data[b"total_cost"])

#         # Start of stock check
#         items = json.loads(order_data[b"items"].decode())
#         revert_order_items = []
#         for item_id in items:
#             if not subtract_stock_quantity(item_id, 1):  # Check if there's enough stock for each item
#                 for item_id in revert_order_items:
#                     add_stock_quantity(item_id, 1)  # Revert the stock changes if there's not enough stock for any item
#                 return jsonify({"error": "Not enough stock"}), 400
#             revert_order_items.append(item_id)
#         # End of stock check

#         # Start of payment processing
#         payment_response = process_payment(user_id, order_id, total_cost)  # Process the payment
#         if payment_response.status_code == 200:
#             pipe.multi()
#             pipe.hset(order_key, "paid", "True")
#             pipe.execute()
#             return jsonify({"status": "success"}), 200
#         else:
#             for item_id in revert_order_items:
#                 add_stock_quantity(item_id, 1)  # Revert the stock changes if the payment fails
#             return (
#                 jsonify({"error": payment_response.text}),
#                 payment_response.status_code,
#             )
#         # End of payment processing
#     except Exception as e:
#         return str(e), 500
#     finally:
#         pipe.unwatch()

@app.post("/checkout/<order_id>")
def checkout(order_id):
    # Fetch the order details with the order_id
    order_key = f"order:{order_id}"
    try:
        pipe = db.pipeline(transaction=True)
        pipe.watch(order_key)
        pipe.multi()
        pipe.hgetall(order_key)
        result = pipe.execute()
        order_data = result[0]
        if not order_data:
            return jsonify({"error": "Order not found"}), 400

        # Parse order details from byte string to string and int
        user_id = int(order_data[b"user_id"])
        item_id = int(order_data[b"item_id"])
        quantity = int(order_data[b"quantity"])
        paid_status = order_data[b"paid"].decode('utf-8')  # 'True' or 'False'
        
        # The order details should now be in a format that can be used by the orchestrator service
        return jsonify({
            "order_id": order_id, 
            "user_id": user_id, 
            "item_id": item_id, 
            "quantity": quantity,
            "paid": paid_status}), 200
    except Exception as e:
        return str(e), 500
    finally:
        pipe.reset()


