from kafka import KafkaConsumer
from sqlalchemy import create_engine, text
import json
import os

KAFKA_BOOTSTRAP = os.getenv(
    "KAFKA_BOOTSTRAP",
    "cloudshop-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092"
)

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:admin123@postgres-postgresql.database.svc.cluster.local:5432/cloudshop"
)

consumer = KafkaConsumer(
    "orders",
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

engine = create_engine(DATABASE_URL)

print("Order Consumer Started")

for message in consumer:

    order = message.value

    print(f"Received: {order}")

    with engine.connect() as conn:

        conn.execute(
            text("""
            insert into kafka_orders
            (order_id, product, qty)
            values
            (:order_id,:product,:qty)
            """),
            {
                "order_id": order["orderId"],
                "product": order["product"],
                "qty": order["qty"]
            }
        )

        conn.commit()

        print("Inserted into DB")
