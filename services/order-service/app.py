from fastapi import FastAPI, HTTPException
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


@app.get("/health")
def health():
    try:
        producer.partitions_for("orders")

        return {
            "status": "UP",
            "service": "order-service",
            "kafka": "UP"
        }

    except Exception as e:
        print(f"Kafka Error: {e}")

        raise HTTPException(
            status_code=503,
            detail={
                "status": "DOWN",
                "service": "order-service",
                "error": str(e)
            }
        )


class Order(BaseModel):
    orderId: int
    product: str
    qty: int


@app.get("/")
def root():
    return {
        "message": "Order Service Running"
    }


@app.post("/orders")
def create_order(order: Order):

    producer.send("orders", order.dict())

    return {
        "status": "sent",
        "order": order.dict()
    }