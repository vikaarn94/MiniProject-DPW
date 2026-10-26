import pandas as pd
import pytest
from contract import CONTRACT_ERRORS, reason_code, validate_record
from pydantic import ValidationError
from validate import build_audit, validate_dataframe

GOOD = {
    "order_id": "O1", "customer_id": "C1", "order_ts": "2026-09-01T09:15:00Z",
    "product_category": "Books", "quantity": 2, "unit_price": 10.0,
    "discount_pct": 0.1, "payment_method": "card", "status": "paid",
}


def bad(**changes):
    return {**GOOD, **changes}


def test_valid_record_passes():
    order = validate_record(GOOD)
    assert order.order_id == "O1" and order.quantity == 2


@pytest.mark.parametrize("record, code", [
    (bad(order_id=""), "order_id:missing"),
    (bad(customer_id=None), "customer_id:missing"),
    (bad(customer_id=float("nan")), "customer_id:missing"),
    (bad(order_ts=None), "order_ts:missing"),
    (bad(quantity=0), "quantity:greater_than"),
    (bad(quantity=-3), "quantity:greater_than"),
    (bad(unit_price=-1), "unit_price:greater_than_equal"),
    (bad(discount_pct=1.2), "discount_pct:less_than_equal"),
    (bad(discount_pct=-0.1), "discount_pct:greater_than_equal"),
    (bad(order_ts="not-a-date"), "order_ts:datetime_from_date_parsing"),
    (bad(status="pending"), "status:literal_error"),
    (bad(extra_field="x"), "extra_field:extra_forbidden"),
])
def test_contract_violations_are_rejected_with_reason(record, code):
    with pytest.raises(CONTRACT_ERRORS) as info:
        validate_record(record)
    assert reason_code(info.value) == code


def test_missing_column_is_rejected():
    record = dict(GOOD)
    del record["payment_method"]
    with pytest.raises(ValueError, match="payment_method"):
        validate_record(record)


def test_wrong_types_are_rejected():
    with pytest.raises(CONTRACT_ERRORS):
        validate_record(bad(quantity="banyak"))
    with pytest.raises(CONTRACT_ERRORS):
        validate_record(bad(unit_price="gratis"))


def test_fractional_quantity_is_not_silently_truncated():
    with pytest.raises(ValueError, match="bilangan bulat"):
        validate_record(bad(quantity=2.5))
    assert validate_record(bad(quantity=3.0)).quantity == 3
    assert validate_record(bad(quantity="4")).quantity == 4


def test_validate_dataframe_reconciles_input_valid_rejected():
    df = pd.DataFrame([
        GOOD,
        bad(order_id="O2", quantity=0),
        bad(order_id="O3", customer_id=None),
        bad(order_id="O4", order_ts=None),
    ])
    df["order_ts"] = pd.to_datetime(df["order_ts"], utc=True, errors="coerce")

    valid, rejected = validate_dataframe(df)
    audit = build_audit(len(df), valid, rejected)

    assert (audit["valid"], audit["rejected"]) == (1, 3)
    assert audit["reconciled"] is True
    assert {r["order_id"] for r in rejected} == {"O2", "O3", "O4"}
    assert sum(audit["rejected_by_reason"].values()) == 3


def test_empty_dataframe_is_reconciled():
    valid, rejected = validate_dataframe(pd.DataFrame(columns=list(GOOD)))
    assert build_audit(0, valid, rejected)["reconciled"] is True


def test_validation_error_type_exported():
    assert ValidationError in CONTRACT_ERRORS
