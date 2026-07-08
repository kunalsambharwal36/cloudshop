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

print("===================================")
print("Starting Order Consumer...")
print("Kafka Bootstrap:", KAFKA_BOOTSTRAP)
print("Database URL:", DATABASE_URL)
print("===================================")

consumer = KafkaConsumer(
    "orders",
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="order-consumer-group"
)

print("KafkaConsumer object created")
print("Partitions:", consumer.partitions_for_topic("orders"))
print("Order Consumer Started")
print("Waiting for messages...")

engine = create_engine(DATABASE_URL)

for message in consumer:
    try:
        order = message.value

        print(f"Received Order: {order}")

        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO kafka_orders
                    (order_id, product, qty)
                    VALUES
                    (:order_id, :product, :qty)
                """),
                {
                    "order_id": order["orderId"],
                    "product": order["product"],
                    "qty": order["qty"]
                }
            )

        print("Order inserted into database successfully.\n")

    except Exception as e:
        print("===================================")
        print("ERROR PROCESSING MESSAGE")
        print(repr(e))
        print("===================================")