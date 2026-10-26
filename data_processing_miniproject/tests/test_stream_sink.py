import duckdb
import pytest
from stream_sink import DUPLICATE, INSERTED, REJECTED, open_sink, process_event

EVENT = {
    "order_id": "TEST-DUPLICATE-001", "customer_id": "CUST-001",
    "order_ts": "2026-10-25T10:00:00", "product_category": "Electronics",
    "quantity": 2, "unit_price": 100000, "discount_pct": 0.1,
    "payment_method": "transfer", "status": "paid",
}


def count(con):
    return con.execute("SELECT COUNT(*) FROM stream_orders").fetchone()[0]


def test_same_event_twice_creates_one_business_transaction():
    con = open_sink(":memory:")

    assert process_event(con, EVENT, 0, 1)[0] == INSERTED
    assert process_event(con, EVENT, 0, 2)[0] == DUPLICATE
    assert count(con) == 1


def test_first_write_wins_for_conflicting_duplicate():
    con = open_sink(":memory:")
    process_event(con, EVENT, 0, 1)
    process_event(con, {**EVENT, "quantity": 99}, 0, 2)

    assert con.execute("SELECT quantity FROM stream_orders").fetchone()[0] == 2


def test_invalid_event_never_reaches_sink():
    con = open_sink(":memory:")
    outcome, order, reason = process_event(con, {**EVENT, "quantity": 0})

    assert outcome == REJECTED and order is None
    assert "quantity" in reason
    assert count(con) == 0


def test_replay_after_crash_is_idempotent(tmp_path):
    """Crash sebelum commit offset => Kafka mengirim ulang pesan yang sama."""
    path = tmp_path / "sink.duckdb"
    events = [{**EVENT, "order_id": f"O{i}"} for i in range(5)]

    con = open_sink(path)
    for offset, event in enumerate(events):
        process_event(con, event, 0, offset)
    con.close()  # "crash": offset tidak sempat di-commit

    con = open_sink(path)  # restart; seluruh pesan dikirim ulang
    outcomes = [process_event(con, e, 0, i)[0] for i, e in enumerate(events)]

    assert outcomes == [DUPLICATE] * 5
    assert count(con) == 5


def test_primary_key_blocks_duplicates_even_with_direct_sql():
    con = open_sink(":memory:")
    process_event(con, EVENT)

    with pytest.raises(duckdb.ConstraintException):
        con.execute(
            "INSERT INTO stream_orders (order_id, customer_id, order_ts, "
            "product_category, quantity, unit_price, discount_pct, "
            "payment_method, status) VALUES "
            "('TEST-DUPLICATE-001','c','2026-01-01','p',1,1,0,'m','paid')"
        )
