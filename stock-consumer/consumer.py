from kafka import KafkaConsumer, KafkaProducer
import json
import os
import redis

# app = Flask("stock-consumer-service")


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
    group_id="stock_consumer_group",
    bootstrap_servers="kafka-service:9092",
    api_version=(0, 11, 5),
    auto_offset_reset="earliest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    key_deserializer=lambda x: json.loads(x.decode("utf-8")),
)


def modify_stock_list(items: list, amount: int):
    print("in modify stock list function line 32", items, amount)
    pipe = db.pipeline(transaction=True)
    # First phase: check all items
    amount = int(amount)
    for item_id in items:
        print("item_id in modify stock list function line 35", item_id)
        item_key = f"item:{item_id}"
        print("item_key in modify stock list function line 37", item_key)
        stock = db.hget(item_key, "stock")
        print("stock variable in modify stock list function line 38", stock)
        if stock is None:
            return (
                {"error": "Item not found: "},
                400,
            )
        print("stock in modify stock list function line 41", stock)
        # + not - because sign of amount indicates the direction of the transaction
        if int(stock) + amount < 0:
            return (
                {"error": "Insufficient stock: "},
                400,
            )

    # Second phase: actual decrement
    try:
        for item_id in items:
            item_key = f"item:{item_id}"
            pipe.hincrby(item_key, "stock", amount)
        pipe.execute()
        return {"done": True}, 200
    except Exception as e:
        return str(e), 500
    finally:
        pipe.reset()


consumer.subscribe(["stock_check_topic"])
for message in consumer:
    print("stock consumer received message")
    msg = message.value
    transaction_id = message.key
    affected_items = msg["affected_items"]

    # reverse_items = []
    if msg["action"] == "add":
        response, status_code = modify_stock_list(affected_items, 1)
        print("received modify_stock_list response", response, status_code)
        # if msg["is_roll_back"]=="false" and db.get(f"transaction:{transaction_id}"):
        #     print(f"Transaction {transaction_id} has been processed before, skipping...")
        #     continue
        # If this is not a rollback operation, store the transaction_id in Redis to mark this operation as processed
        if (
            msg["is_roll_back"] == "false"
            and db.get(f"transaction:{transaction_id}") is None
        ):
            db.set(f"transaction:{transaction_id}", 1)
        if status_code != 200:
            producer.send(
                "stock_check_result_topic",
                key=transaction_id,
                value={
                    "status": "failure",
                    "affected_items": affected_items,
                    "is_roll_back": msg["is_roll_back"],
                    "action": "add",
                },
            )
            print("send failure message to stock_check_result_topic")
        else:
            producer.send(
                "stock_check_result_topic",
                key=transaction_id,
                value={
                    "status": "success",
                    "affected_items": affected_items,
                    "is_roll_back": msg["is_roll_back"],
                    "action": "add",
                },
            )
            print("send success message to stock_check_result_topic")
        # reverse_items.append(item_id)
    elif msg["action"] == "remove":
        response, status_code = modify_stock_list(affected_items, -1)
        print("received modify_stock_list response", response, status_code)
        if status_code != 200:
            producer.send(
                "stock_check_result_topic",
                key=transaction_id,
                value={
                    "status": "failure",
                    "affected_items": affected_items,
                    "is_roll_back": msg["is_roll_back"],
                    "action": "remove",
                },
            )
            print("send failure message to stock_check_result_topic remove")
        else:
            producer.send(
                "stock_check_result_topic",
                key=transaction_id,
                value={
                    "status": "success",
                    "affected_items": affected_items,
                    "is_roll_back": msg["is_roll_back"],
                    "action": "remove",
                },
            )
            print("send success message to stock_check_result_topic remove")
        # reverse_items.append(item_id)
