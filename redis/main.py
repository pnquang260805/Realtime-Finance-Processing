from fastapi import FastAPI, HTTPException
import redis

app = FastAPI()

host = "redis"
port = "6379"

red = redis.Redis(
    host=host,
    port=port,
    db=0,
    decode_responses=True
)


@app.post("/q")
def set_key(key: str, value: str):
    red.set(key, value)


@app.get("/q")
def get_key(key: str):
    val = red.get(key)
    if val is not None:
        return {"key": key, "value": val}
    else:
        raise HTTPException(status_code=400, detail="Key not found")
