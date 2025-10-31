import json
import websocket

from typing import List

from services.kafka_service import KafkaService


class FinnhubService:
    def __init__(self, token: str, symbols: List[str], output_topic: str, kafka_service: KafkaService):
        self.symbols = symbols
        self.kafka_service = kafka_service
        self.kafka_output_topic = output_topic
        self.token = token

    def __on_message(self, ws, message):
        try:
            data = json.loads(message)
            if data["type"] == "ping":
                return
            serialized = json.dumps(data)
            self.kafka_service.send_message(
                serialized, self.kafka_output_topic)
        except Exception as e:
            print(e)

    def __on_error(self, ws, error):
        print(f"Error: {error}")

    def __on_close(self, ws, close_status_code, close_msg):
        print("Connection closed")

    def __on_open(self, ws):
        for symbol in self.symbols:
            ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))

    def run(self):
        socket = f"wss://ws.finnhub.io?token={self.token}"
        ws = websocket.WebSocketApp(
            socket, on_close=self.__on_close, on_error=self.__on_error, on_message=self.__on_message)
        ws.on_open = self.__on_open
        ws.run_forever()
