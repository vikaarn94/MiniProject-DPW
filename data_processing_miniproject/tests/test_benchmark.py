import benchmark
import pandas as pd


def make_valid(path, rows=30):
    pd.DataFrame({
        "order_id": [f"O{i}" for i in range(rows)],
        "order_ts": ["2026-09-01T09:15:00"] * rows,
        "product_category": ["Books", "Home", "Fashion"] * (rows // 3),
        "quantity": [1 + i % 4 for i in range(rows)],
        "unit_price": [10.0 + i for i in range(rows)],
        "discount_pct": [0.1 * (i % 3) for i in range(rows)],
    }).to_parquet(path)


def test_three_implementations_give_equivalent_results(tmp_path):
    path = tmp_path / "valid.parquet"
    make_valid(path)

    pandas_result = benchmark.run_pandas(path)
    for function in (benchmark.run_polars_eager, benchmark.run_polars_lazy):
        assert benchmark.results_equivalent(pandas_result, function(path))


def test_make_dataset_has_requested_unique_rows(tmp_path):
    base = tmp_path / "valid.parquet"
    make_valid(base)
    big = benchmark.make_dataset(base, 100, tmp_path / "big.parquet")

    df = pd.read_parquet(big)
    assert len(df) == 100 and df["order_id"].is_unique


def test_results_equivalent_detects_difference():
    a = pd.DataFrame({"product_category": ["A"], "order_date": ["2026-09-01"], "net_amount": [1.0]})
    b = pd.DataFrame({"product_category": ["A"], "order_date": ["2026-09-01"], "net_amount": [2.0]})
    assert not benchmark.results_equivalent(a, b)
