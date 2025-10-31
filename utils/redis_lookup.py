import requests
from datetime import datetime


class RedisLookup:
    def __init__(self):
        host = "redis-api"
        post = "8000"
        self.base_redis_url = f"http://{host}:{post}/q?"

    def __build_symbol_key(self, symbol: str, type: str) -> str:
        """
            Build a key with format [date]-[symbol]-[type]
            Example: 20251031-AAPL-high
        """
        now = datetime.now()
        return f"{now.strftime('%Y%m%d')}-{symbol}-{type}"

    def get_value(self, symbol: str, type: str) -> str:
        req = requests.get(
            f"{self.base_redis_url}key={self.__build_symbol_key(symbol, type)}")
        data = req.json()
        value = data.get("value")
        if value is not None:
            return value
        else:
            print("NONE")

    def set_value(self, symbol: str, type: str, value) -> None:
        requests.post(
            f"{self.base_redis_url}key={self.__build_symbol_key(symbol, type)}&value={value}")
