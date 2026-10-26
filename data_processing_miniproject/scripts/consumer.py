
import json
from pathlib import Path
from datetime import datetime
from typing import Literal

import pandas as pd
from kafka import KafkaConsumer
from pydantic import BaseModel, ConfigDict, Field, ValidationError


BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "orders"
GROUP_ID = "orders-validation-group"

VALID_PATH = Path("data/stream/valid.jsonl")
REJECTED_PATH = Path("data/stream/rejected.jsonl")


class Order(BaseModel):
    model_config = ConfigDict(extra="forbid")

    order_id: str = Field(min_length=1)
    customer_id: str = Field(min_length=1)
    order_ts: datetime
    product_category: str = Field(min_length=1)
    quantity: int = Field(gt=0)
    unit_price: float = Field(ge=0)
    discount_pct: float = Field(ge=0, le=1)
    payment_method: str = Field(min_length=1)

    status: Literal[
        "paid",
        "shipped",
        "cancelled",
        "refunded"
    ]


def is_blank(value):
    return (
        value is None
        or pd.isna(value)
        or str(value).strip() == ""
    )


def validate_event(payload):
    required_fields = [
        "order_id",
        "customer_id",
        "order_ts",
        "product_category",
        "quantity",
        "unit_price",
        "discount_pct",
        "payment_method",
        "status",
    ]

    for field in required_fields:
        if field not in payload or is_blank(payload[field]):
            raise ValueError(f"Field wajib kosong: {field}")

    payload["quantity"] = int(payload["quantity"])
    payload["unit_price"] = float(payload["unit_price"])
    payload["discount_pct"] = float(payload["discount_pct"])

    return Order.model_validate(payload)


def write_jsonl(path, record):
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("a", encoding="utf-8") as file:
        file.write(
            json.dumps(
                record,
                ensure_ascii=False,
                default=str
            ) + "\n"
        )


def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        ),
        consumer_timeout_ms=15000,
    )

    valid_count = 0
    rejected_count = 0

    try:
        for message in consumer:
            payload = message.value

            metadata = {
                "topic": message.topic,
                "partition": message.partition,
                "offset": message.offset,
            }

            try:
                order = validate_event(payload)

                write_jsonl(
                    VALID_PATH,
                    {
                        **metadata,
                        "order": order.model_dump(mode="json"),
                    },
                )

                valid_count += 1
                print(
                    f"VALID: {order.order_id} | "
                    f"partition={message.partition} | "
                    f"offset={message.offset}"
                )

            except (
                ValidationError,
                ValueError,
                TypeError,
            ) as exc:
                write_jsonl(
                    REJECTED_PATH,
                    {
                        **metadata,
                        "payload": payload,
                        "error": str(exc),
                    },
                )

                rejected_count += 1
                print(
                    f"REJECTED: "
                    f"{payload.get('order_id')} | {exc}"
                )

            # Commit setelah penulisan file selesai.
            consumer.commit()

        print("\n=== RINGKASAN CONSUMER ===")
        print("Valid   :", valid_count)
        print("Rejected:", rejected_count)
        print("File valid   :", VALID_PATH)
        print("File rejected:", REJECTED_PATH)

    finally:
        consumer.close()


if __name__ == "__main__":
    main()
