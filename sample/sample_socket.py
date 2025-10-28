from dotenv import load_dotenv
import os

import json
import requests
import websocket
import json
from confluent_kafka import Producer

conf = {
    "bootstrap.servers": "localhost:9092",
    "security.protocol": "PLAINTEXT"
}

producer = Producer(conf)

load_dotenv()

API_KEY = os.getenv("FINHUB_TOKEN")

topic = "raw-trade-topic"


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


def on_message(ws, message):
    try:
        data = json.loads(message)
        if data["type"] != "ping":
            serialized = json.dumps(data)
            try:
                producer.produce(topic, value=serialized,
                                 callback=delivery_report)
                producer.flush()
                print("Sent successfully.")
            except Exception as e:
                print(f"Failed to send message: {e}")

    except json.JSONDecodeError:
        print("Invalid JSON:", message)
        return

    # print(data)


def on_error(ws, error):
    print("Error:", error)


def on_close(ws, close_status_code, close_msg):
    print("Connection closed")


def on_open(ws):
    print("Start connect")
    ws.send(json.dumps({"type": "subscribe", "symbol": "AAPL"}))
    ws.send(json.dumps({"type": "subscribe", "symbol": "AMZN"}))
    ws.send(json.dumps({"type": "subscribe", "symbol": "MSFT"}))
    ws.send(json.dumps({"type": "subscribe", "symbol": "GOOGL"}))
    ws.send(json.dumps({"type": "subscribe", "symbol": "META"}))


socket = f"wss://ws.finnhub.io?token={API_KEY}"
ws = websocket.WebSocketApp(socket,
                            on_message=on_message,
                            on_error=on_error,
                            on_close=on_close)
ws.on_open = on_open
ws.run_forever()
