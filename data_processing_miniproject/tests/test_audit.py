import json

import duckdb
import pandas as pd
from audit_pipeline import run_checks


def build_fixture(tmp_path, leak=False):
    pd.DataFrame({"order_id": ["O1", "O2", "O3"]}).to_csv(
        tmp_path / "orders.csv", index=False
    )
    pd.DataFrame({"order_id": ["O1", "O2", "O3"]}).to_parquet(tmp_path / "raw.parquet")
    pd.DataFrame({"order_id": ["O1", "O2"]}).to_parquet(tmp_path / "valid.parquet")

    (tmp_path / "rejected.json").write_text(
        json.dumps([{"order_id": "O3"}]), encoding="utf-8"
    )

    rows = [
        ("O1", "C1", "Books", 2, 10.0, 0.1, 20.0, 18.0, "paid"),
        ("O2", "C2", "Books", 1, 5.0, 0.0, 5.0, 5.0, "cancelled"),
    ]
    if leak:
        rows.append(("O3", "C1", "Books", 1, 1.0, 0.0, 1.0, 1.0, "paid"))

    con = duckdb.connect(str(tmp_path / "wh.duckdb"))
    con.execute(
        "CREATE TABLE fct_orders (order_id VARCHAR, customer_id VARCHAR, "
        "product_category VARCHAR, quantity INT, unit_price DOUBLE, "
        "discount_pct DOUBLE, gross_amount DOUBLE, net_amount DOUBLE, status VARCHAR)"
    )
    con.executemany("INSERT INTO fct_orders VALUES (?,?,?,?,?,?,?,?,?)", rows)
    con.execute("CREATE TABLE dim_customer AS SELECT * FROM (VALUES ('C1'),('C2')) t(customer_id)")
    con.execute("CREATE TABLE dim_product_category AS SELECT 'Books' AS product_category")
    net = 18.0 + (1.0 if leak else 0.0)
    con.execute(f"CREATE TABLE mart_daily_sales AS SELECT {net} AS net_sales")
    con.close()

    return (tmp_path / "orders.csv", tmp_path / "raw.parquet",
            tmp_path / "valid.parquet", tmp_path / "rejected.json",
            tmp_path / "wh.duckdb")


def test_clean_pipeline_passes_all_checks(tmp_path):
    checks = run_checks(*build_fixture(tmp_path))
    assert all(ok for _, ok, _ in checks), checks


def test_rejected_order_leaking_into_fact_table_is_detected(tmp_path):
    checks = {name: ok for name, ok, _ in run_checks(*build_fixture(tmp_path, leak=True))}
    assert checks["order_id rejected tidak ada di fct_orders"] is False
