from confluent_kafka import Producer


class KafkaService:
    def init_producer(self, bootstrap, **kwargs):
        self.conf = {
            "bootstrap.servers": bootstrap,
            **kwargs
        }
        self.producer = Producer(self.conf)

    def __report(self, err, msg):
        if err is not None:
            print(f"Fail to send message with error: {err}")
        else:
            print(
                f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

    def send_message(self, message, topic):
        try:
            self.producer.produce(topic, value=message, callback=self.__report)
            self.producer.flush()
            print("Sent successfully.")
        except Exception as e:
            print(f"Failed to send message: {e}")
