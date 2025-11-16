import os
import requests
import json
import logging

from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
TOKEN = os.getenv("FINNHUB_TOKEN")

def build_key(symb : str) -> str:
    today = datetime.now().strftime("%y%m%d")
    return f"{today}-{symb}-pc"

def main():
    symbols = ["AAPL", "META", "AMZN"]
    redis_url = "http://redis-api:8000/q?"
    for symbol in symbols:
        quote_url = f"https://finnhub.io/api/v1/quote?symbol={symbol}&token={TOKEN}"
        key = build_key(symbol)
        req = requests.get(quote_url)
        if req.status_code != 200:
            return
        data = req.json()
        pc = data.get("pc", 0.0)
        if pc == 0.0:
            return
        redis_full_url = f"{redis_url}key={key}&value={pc}"
        requests.post(redis_full_url)

if __name__ == "__main__":
    main()
