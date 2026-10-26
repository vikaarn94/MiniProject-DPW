"""Sink idempoten untuk event streaming: DuckDB dengan PRIMARY KEY order_id."""

from pathlib import Path

import duckdb
from contract import CONTRACT_ERRORS, reason_code, validate_record

SINK_PATH = Path("data/stream/stream_orders.duckdb")

INSERTED = "inserted"
DUPLICATE = "duplicate"
REJECTED = "rejected"

CREATE_TABLE = """
    CREATE TABLE IF NOT EXISTS stream_orders (
        order_id         VARCHAR PRIMARY KEY,
        customer_id      VARCHAR NOT NULL,
        order_ts         TIMESTAMP NOT NULL,
        product_category VARCHAR NOT NULL,
        quantity         INTEGER NOT NULL,
        unit_price       DOUBLE NOT NULL,
        discount_pct     DOUBLE NOT NULL,
        payment_method   VARCHAR NOT NULL,
        status           VARCHAR NOT NULL,
        kafka_partition  INTEGER,
        kafka_offset     BIGINT,
        ingested_at      TIMESTAMP DEFAULT current_timestamp
    )
"""


def open_sink(path=SINK_PATH):
    """Buka (dan buat bila perlu) sink. Gunakan ':memory:' untuk pengujian."""
    if str(path) != ":memory:":
        Path(path).parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(path))
    con.execute(CREATE_TABLE)

    return con


def process_event(con, payload, partition=None, offset=None):
    """Validasi event lalu tulis ke sink secara idempoten.

    Kembalian:
      (INSERTED, order, None)       event baru masuk ke sink
      (DUPLICATE, order, None)      order_id sudah ada; tidak ditulis lagi
      (REJECTED, None, kode_alasan) melanggar kontrak, tidak masuk sink

    Penulisan satu event adalah satu pernyataan INSERT ... ON CONFLICT DO
    NOTHING, sehingga atomik: setelah crash, event ada utuh atau tidak
    sama sekali. Event yang dikirim ulang oleh Kafka (at-least-once) tidak
    menggandakan transaksi. Kebijakan: first-write-wins berdasarkan order_id.
    """
    try:
        order = validate_record(payload)

    except CONTRACT_ERRORS as exc:
        return REJECTED, None, f"{reason_code(exc)} | {exc}"

    inserted = con.execute(
        """
        INSERT INTO stream_orders (
            order_id, customer_id, order_ts, product_category,
            quantity, unit_price, discount_pct, payment_method,
            status, kafka_partition, kafka_offset
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (order_id) DO NOTHING
        RETURNING order_id
        """,
        [
            order.order_id,
            order.customer_id,
            order.order_ts,
            order.product_category,
            order.quantity,
            order.unit_price,
            order.discount_pct,
            order.payment_method,
            order.status,
            partition,
            offset,
        ],
    ).fetchall()

    return (INSERTED if inserted else DUPLICATE), order, None
