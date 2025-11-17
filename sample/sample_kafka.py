import json
import random
import time

from confluent_kafka import Producer

conf = {
    "bootstrap.servers": "localhost:9092",
    "security.protocol": "PLAINTEXT"
}

producer = Producer(conf)

def msg():
    return {
    "data": [
        {
            "c": [
                "1",
                "8"
            ],
            "p": random.random() * 100,
            "s": "AAPL",
            "t": 1761230062740,
            "v": 100
        },
        {
            "c": [
                "1",
                "8"
            ],
            "p": random.random() * 100,
            "s": "AAPL",
            "t": 1761230062740,
            "v": 100
        }
    ],
    "type": "trade"
}

topic = "raw-trade-topic"


def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    else:
        print(
            f"✅ Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


print("📤 Sending message ...")

try:
    n = random.randint(1, 1000)
    print(n)
    for i in range(n):
        data = msg()
        serialized = json.dumps(data)
        producer.produce(topic, value=serialized, callback=delivery_report)
        producer.flush()
        print(f"✅ Sent successfully. {i+1}")
        time.sleep(2)
except Exception as e:
    print(f"⚠️ Failed to send message: {e}")
