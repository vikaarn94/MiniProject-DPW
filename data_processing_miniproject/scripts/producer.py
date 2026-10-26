
import json
import time
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer


BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "orders"
SOURCE = Path("data/orders.csv")


def json_serializer(value):
    return json.dumps(value, default=str).encode("utf-8")


def main():
    if not SOURCE.exists():
        raise FileNotFoundError(
            f"Dataset tidak ditemukan: {SOURCE}"
        )

    df = pd.read_csv(SOURCE)

    # Ambil minimal 20 record untuk demo.
    records = df.head(20).to_dict(orient="records")

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=json_serializer,
        key_serializer=lambda key: key.encode("utf-8"),
        acks="all",
        retries=3,
    )

    try:
        for record in records:
            order_id = str(record.get("order_id", ""))

            future = producer.send(
                TOPIC,
                key=order_id,
                value=record,
            )

            metadata = future.get(timeout=30)

            print(
                f"Event terkirim | "
                f"order_id={order_id} | "
                f"partition={metadata.partition} | "
                f"offset={metadata.offset}"
            )

            time.sleep(0.1)

        producer.flush()

        print(f"\nSelesai mengirim {len(records)} event.")

    finally:
        producer.close()


if __name__ == "__main__":
    main()
