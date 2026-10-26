
import json
import os

from kafka import KafkaProducer

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "orders")

# Event yang sama akan dikirim dua kali
event = {
    "order_id": "TEST-DUPLICATE-001",
    "customer_id": "CUST-001",
    "order_ts": "2026-10-25T10:00:00",
    "product_category": "Electronics",
    "quantity": 2,
    "unit_price": 100000,
    "discount_pct": 0.1,
    "payment_method": "transfer",
    "status": "paid",
}

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    key_serializer=lambda key: key.encode("utf-8"),
    acks="all",
    retries=3,
)

try:
    for i in range(2):
        result = producer.send(
            TOPIC,
            key=event["order_id"],
            value=event,
        ).get(timeout=30)

        print(f"Event ke-{i + 1} berhasil dikirim")
        print(f"Order ID : {event['order_id']}")
        print(f"Partition: {result.partition}")
        print(f"Offset   : {result.offset}")
        print("-" * 30)

finally:
    producer.close()

print("Pengujian duplikasi selesai.")
