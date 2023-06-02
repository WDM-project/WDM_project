from flask import Flask, jsonify, request
import os
import requests

app = Flask("coord-service")


order_service_url = os.environ["GATEWAY_URL"]
stock_service_url = os.environ["GATEWAY_URL"]
payment_service_url = os.environ["GATEWAY_URL"]


# Persistent storage for saga states, for simplicity we are using a dict here.
sagas = {}


def create_order():
    order_data = request.json
    saga_id = len(sagas) + 1  # generate a new saga id
    sagas[saga_id] = {"status": "started", "steps": []}

    try:
        # Step 1: Create Order
        order_response = requests.post(f"{order_service_url}/orders/create_order", json=order_data)
        order_response.raise_for_status()
        order_id = order_response.json()['order_id']
        sagas[saga_id]['steps'].append({"service": "order", "status": "completed", "action": "create_order", "data": order_id})

        # Step 2: Decrease stock
        stock_response = requests.post(f"{stock_service_url}/stock/subtract/{order_data['item_id']}/{order_data['quantity']}")
        stock_response.raise_for_status()
        sagas[saga_id]['steps'].append({"service": "stock", "status": "completed", "action": "subtract", "data": {"item_id": order_data['item_id'], "amount": order_data['quantity']}})

        # Step 3: Make payment
        payment_data = {"user_id": order_data['user_id'], "order_id": order_id, "amount": order_data['amount']}
        payment_response = requests.post(f"{payment_service_url}/payment/pay/{order_data['user_id']}/{order_id}/{order_data['amount']}", json=payment_data)
        payment_response.raise_for_status()
        sagas[saga_id]['steps'].append({"service": "payment", "status": "completed", "action": "pay", "data": payment_data})

        sagas[saga_id]['status'] = "completed"
        return jsonify({"status": "success", "order_id": order_id}), 200
    except Exception as e:
        sagas[saga_id]['status'] = "failed"
        # If any step fails, initiate compensating transactions for completed steps in reverse order
        for step in reversed(sagas[saga_id]['steps']):
            if step['status'] == "completed":
                try:
                    # Send compensating request to corresponding service
                    if step['service'] == "order":
                        requests.post(f"{order_service_url}/orders/delete_order", json={"order_id": step['data']})
                    elif step['service'] == "stock":
                        requests.post(f"{stock_service_url}/stock/add/{step['data']['item_id']}/{step['data']['amount']}")
                    elif step['service'] == "payment":
                        requests.post(f"{payment_service_url}/payment/refund/{step['data']['user_id']}/{step['data']['order_id']}/{step['data']['amount']}")
                    step['status'] = "compensated"
                except Exception as comp_e:
                    return str(comp_e), 500
        return str(e), 500
