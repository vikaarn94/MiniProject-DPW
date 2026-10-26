
import json
from pathlib import Path
from datetime import datetime
from typing import Literal

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, ValidationError


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


def validate_record(record):
    # Cek nilai kosong sebelum validasi Pydantic.
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
        if field not in record or is_blank(record[field]):
            raise ValueError(
                f"Field wajib kosong: {field}"
            )

    # Konversi tipe data dari CSV.
    record["quantity"] = int(record["quantity"])
    record["unit_price"] = float(record["unit_price"])
    record["discount_pct"] = float(record["discount_pct"])

    return Order.model_validate(record)


def main():
    source_path = Path("data/raw/orders.parquet")
    valid_dir = Path("data/valid")
    rejected_dir = Path("data/rejected")

    valid_dir.mkdir(parents=True, exist_ok=True)
    rejected_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(source_path)

    valid_orders = []
    rejected_orders = []

    for index, row in df.iterrows():
        original = row.to_dict()

        # Normalisasi timestamp menjadi string ISO.
        if pd.notna(original.get("order_ts")):
            original["order_ts"] = (
                pd.Timestamp(original["order_ts"]).isoformat()
            )
        else:
            original["order_ts"] = None

        try:
            order = validate_record(original)

            valid_orders.append(
                order.model_dump(mode="json")
            )

        except (
            ValidationError,
            ValueError,
            TypeError,
        ) as exc:
            rejected_orders.append({
                "row_number": int(index) + 2,
                "order_id": original.get("order_id"),
                "payload": original,
                "error": str(exc),
            })

    valid_path = valid_dir / "orders_valid.parquet"
    rejected_path = rejected_dir / "orders_rejected.json"

    pd.DataFrame(valid_orders).to_parquet(
        valid_path,
        index=False
    )

    rejected_path.write_text(
        json.dumps(
            rejected_orders,
            indent=2,
            ensure_ascii=False,
            default=str
        ),
        encoding="utf-8"
    )

    print("\n=== HASIL VALIDASI ===")
    print("Total record :", len(df))
    print("Valid        :", len(valid_orders))
    print("Rejected     :", len(rejected_orders))
    print("File valid   :", valid_path)
    print("File rejected:", rejected_path)

    print("\nContoh alasan penolakan:")

    for item in rejected_orders[:5]:
        print(
            item["order_id"],
            "->",
            item["error"]
        )


if __name__ == "__main__":
    main()
