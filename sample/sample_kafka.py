import json
from confluent_kafka import Producer

conf = {
    "bootstrap.servers": "localhost:9092",
    "security.protocol": "PLAINTEXT"
}

producer = Producer(conf)

data = {
    "data": [
        {
            "c": [
                "1",
                "8"
            ],
            "p": 259.32,
            "s": "AAPL",
            "t": 1761230062740,
            "v": 100
        },
        {
            "c": [
                "1",
                "8"
            ],
            "p": 259.32,
            "s": "AAPL",
            "t": 1761230062740,
            "v": 100
        }
    ],
    "type": "trade"
}

serialized = json.dumps(data)
topic = "raw-trade-topic"


def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    else:
        print(
            f"✅ Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


print("📤 Sending message ...")

try:
    producer.produce(topic, value=serialized, callback=delivery_report)
    producer.flush()
    print("✅ Sent successfully.")
except Exception as e:
    print(f"⚠️ Failed to send message: {e}")
