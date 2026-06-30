from fastapi import FastAPI
from kafka import KafkaProducer
from pydantic import BaseModel
import json
import os

app = FastAPI()

KAFKA_BOOTSTRAP = os.getenv(
    "KAFKA_BOOTSTRAP",
    "localhost:9092"
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

class Order(BaseModel):
    orderId: int
    product: str
    qty: int

@app.get("/")
def root():
    return {"message": "Order Service Running"}

@app.post("/orders")
def create_order(order: Order):

    producer.send("orders", order.dict())

    return {
        "status": "sent",
        "order": order.dict()
    }
