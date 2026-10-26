"""Data contract bersama untuk jalur batch (validate.py) dan streaming (consumer.py)."""

from datetime import datetime
from typing import Literal

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, ValidationError

REQUIRED_FIELDS = [
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

# Error yang berarti "data melanggar kontrak" (bukan bug program).
CONTRACT_ERRORS = (ValidationError, ValueError, TypeError)


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


def _to_int(value):
    """Konversi ke int tanpa membuang pecahan (2.5 tidak boleh menjadi 2)."""
    number = float(value)

    if not number.is_integer():
        raise ValueError(f"quantity harus bilangan bulat, bukan {value!r}")

    return int(number)


def validate_record(record):
    """Validasi satu record; melempar ValueError/ValidationError bila melanggar."""
    record = dict(record)

    # Cek nilai kosong sebelum validasi Pydantic.
    for field in REQUIRED_FIELDS:
        if field not in record or is_blank(record[field]):
            raise ValueError(f"Field wajib kosong: {field}")

    # Konversi tipe data dari CSV/JSON.
    record["quantity"] = _to_int(record["quantity"])
    record["unit_price"] = float(record["unit_price"])
    record["discount_pct"] = float(record["discount_pct"])

    return Order.model_validate(record)


def reason_code(exc):
    """Ringkas alasan penolakan menjadi kode pendek untuk rekap audit."""
    if isinstance(exc, ValidationError):
        first = exc.errors()[0]
        location = ".".join(str(part) for part in first["loc"])
        return f"{location}:{first['type']}"

    message = str(exc)

    if message.startswith("Field wajib kosong: "):
        return message.replace("Field wajib kosong: ", "") + ":missing"

    return "type_error"
