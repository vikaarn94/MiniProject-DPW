"""Integration test lintas komponen: Kafka -> consumer -> sink idempoten.

Di-skip otomatis bila broker tidak tersedia (mis. Docker belum berjalan).
"""
import json
import os
import uuid

import pytest
from stream_sink import DUPLICATE, INSERTED, open_sink, process_event

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

kafka = pytest.importorskip("kafka")


def broker_available():
    try:
        producer = kafka.KafkaProducer(
            bootstrap_servers=BOOTSTRAP,
            request_timeout_ms=3000,
        )
        producer.close()
        return True
    except Exception:  # noqa: BLE001 - jenis error koneksi beragam; semuanya berarti "tidak tersedia"
        return False


pytestmark = [
    pytest.mark.kafka,
    pytest.mark.skipif(not broker_available(), reason="broker Kafka tidak tersedia"),
]


def consume_all(topic, group):
    consumer = kafka.KafkaConsumer(
        topic, bootstrap_servers=BOOTSTRAP, group_id=group,
        auto_offset_reset="earliest", enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=8000,
    )
    try:
        return [(m.partition, m.offset, m.value) for m in consumer]
    finally:
        consumer.close()


def test_duplicate_event_through_real_broker_creates_one_row():
    topic = f"orders-it-{uuid.uuid4().hex[:8]}"
    event = {
        "order_id": f"IT-{uuid.uuid4().hex[:6]}", "customer_id": "C1",
        "order_ts": "2026-10-25T10:00:00", "product_category": "Books",
        "quantity": 1, "unit_price": 10.0, "discount_pct": 0.0,
        "payment_method": "card", "status": "paid",
    }

    producer = kafka.KafkaProducer(
        bootstrap_servers=BOOTSTRAP, acks="all",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )
    for _ in range(2):
        producer.send(topic, key=event["order_id"], value=event).get(timeout=30)
    producer.close()

    sink = open_sink(":memory:")

    first = [process_event(sink, v, p, o)[0] for p, o, v in consume_all(topic, "g1")]
    assert first == [INSERTED, DUPLICATE]

    # Consumer group baru membaca ulang dari awal (mensimulasikan redelivery).
    replay = [process_event(sink, v, p, o)[0] for p, o, v in consume_all(topic, "g2")]
    assert replay == [DUPLICATE, DUPLICATE]

    assert sink.execute("SELECT COUNT(*) FROM stream_orders").fetchone()[0] == 1
