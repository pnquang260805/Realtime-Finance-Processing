import os
from dotenv import load_dotenv

from services.finnhub_service import FinnhubService
from services.kafka_service import KafkaService
load_dotenv()

FINNHUB_KEY = os.getenv("FINHUB_TOKEN")
print(FINNHUB_KEY)

symbols = ["AAPL", "META", "AMZN"]

kafka_service = KafkaService()
kafka_service.init_producer("localhost:9092")

finnhub_service = FinnhubService(FINNHUB_KEY, symbols, "raw-trade-topic", kafka_service)
finnhub_service.run()