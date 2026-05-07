import os
import time
from fastapi import FastAPI, Request

app = FastAPI()

DELAY_SECONDS = float(os.getenv("MOCK_DELAY_SECONDS", "2"))
ITEM_COUNT = int(os.getenv("MOCK_ITEM_COUNT", "1000"))


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/extract")
def extract():
    time.sleep(DELAY_SECONDS)

    items = [
        {
            "id": i,
            "brand": "Honda" if i % 2 == 0 else "Toyota",
            "model": "Civic" if i % 2 == 0 else "Corolla",
            "year": 2020 + (i % 5),
            "price": 15000 + (i * 7 % 40000),
            "mileage": 5000 + (i * 13 % 120000),
        }
        for i in range(ITEM_COUNT)
    ]

    return {"items": items, "count": len(items)}


@app.post("/load")
async def load(request: Request):
    payload = await request.json()
    time.sleep(DELAY_SECONDS)

    return {
        "ok": True,
        "received": len(payload.get("items", [])),
    }
