import pandas as pd
import pytest
from batch_ingest import CsvPageSource, get_page
from retry_utils import TransientSourceError, with_retry


def make_flaky(failures, error=None):
    error = error or TransientSourceError("503")
    state = {"calls": 0}

    def func():
        state["calls"] += 1
        if state["calls"] <= failures:
            raise error
        return "ok"

    return func, state


def test_success_after_transient_failures_uses_exponential_delays():
    func, state = make_flaky(2)
    delays = []

    result = with_retry(func, max_attempts=4, base_delay=1.0,
                        sleep=delays.append, log=lambda *_: None)

    assert result == "ok"
    assert state["calls"] == 3
    assert delays == [1.0, 2.0]


def test_permanent_failure_stops_after_max_attempts():
    func, state = make_flaky(99)
    delays = []

    with pytest.raises(TransientSourceError):
        with_retry(func, max_attempts=3, base_delay=1.0,
                   sleep=delays.append, log=lambda *_: None)

    assert state["calls"] == 3
    assert delays == [1.0, 2.0]


def test_non_retryable_error_is_not_retried():
    func, state = make_flaky(99, error=ValueError("data salah"))
    delays = []

    with pytest.raises(ValueError):
        with_retry(func, max_attempts=5, sleep=delays.append, log=lambda *_: None)

    assert state["calls"] == 1
    assert delays == []


def test_delay_is_capped_by_max_delay():
    func, _ = make_flaky(5)
    delays = []

    with_retry(func, max_attempts=6, base_delay=1.0, max_delay=3.0,
               sleep=delays.append, log=lambda *_: None)

    assert delays == [1.0, 2.0, 3.0, 3.0, 3.0]


def test_retry_does_not_lose_or_duplicate_rows():
    df = pd.DataFrame({"order_id": [f"O{i}" for i in range(25)]})
    source = CsvPageSource(df, simulate_failures=2)
    pages, page = [], 0

    while True:
        chunk = get_page(source, page, 10, sleep=lambda _: None)
        if chunk.empty:
            break
        pages.append(chunk)
        page += 1

    combined = pd.concat(pages, ignore_index=True)
    assert list(combined["order_id"]) == list(df["order_id"])
    assert [len(p) for p in pages] == [10, 10, 5]
