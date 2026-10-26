"""Audit rekonsiliasi end-to-end: sumber -> raw -> valid/rejected -> warehouse."""

import json
from pathlib import Path

import duckdb
import pandas as pd

SOURCE = Path("data/orders.csv")
RAW = Path("data/raw/orders.parquet")
VALID = Path("data/valid/orders_valid.parquet")
REJECTED = Path("data/rejected/orders_rejected.json")
WAREHOUSE = Path("data/warehouse/ecommerce.duckdb")


def run_checks(source, raw, valid, rejected, warehouse):
    """Kembalikan daftar (nama_cek, lolos, detail)."""
    src = pd.read_csv(source)
    raw_df = pd.read_parquet(raw)
    valid_df = pd.read_parquet(valid)
    rejected_rows = json.loads(Path(rejected).read_text(encoding="utf-8"))
    rejected_ids = {r["order_id"] for r in rejected_rows if r["order_id"]}

    checks = []

    def add(name, ok, detail):
        checks.append((name, bool(ok), detail))

    unique_source = src["order_id"].nunique()
    add(
        "sumber (order_id unik) = raw",
        unique_source == len(raw_df),
        f"{unique_source} vs {len(raw_df)}",
    )
    add(
        "raw = valid + rejected",
        len(raw_df) == len(valid_df) + len(rejected_rows),
        f"{len(raw_df)} vs {len(valid_df)} + {len(rejected_rows)}",
    )
    add(
        "valid dan rejected tidak beririsan",
        not (set(valid_df["order_id"]) & rejected_ids),
        f"irisan = {sorted(set(valid_df['order_id']) & rejected_ids)}",
    )

    con = duckdb.connect(str(warehouse), read_only=True)

    try:
        fct = con.execute("SELECT * FROM fct_orders").df()
        fct_ids = set(fct["order_id"])

        add(
            "fct_orders = jumlah order_id valid unik",
            len(fct) == valid_df["order_id"].nunique(),
            f"{len(fct)} vs {valid_df['order_id'].nunique()}",
        )
        add(
            "order_id rejected tidak ada di fct_orders",
            not (fct_ids & rejected_ids),
            f"bocor = {sorted(fct_ids & rejected_ids)}",
        )
        add(
            "fct_orders.order_id unik",
            fct["order_id"].is_unique,
            f"{len(fct)} baris",
        )

        gross = fct["quantity"] * fct["unit_price"]
        net = gross * (1 - fct["discount_pct"])
        add(
            "gross_amount dan net_amount sesuai rumus",
            (abs(fct["gross_amount"] - gross) < 1e-9).all()
            and (abs(fct["net_amount"] - net) < 1e-9).all(),
            "quantity x unit_price; gross x (1 - discount_pct)",
        )

        orphan_customers = con.execute("""
            SELECT COUNT(*) FROM fct_orders f
            LEFT JOIN dim_customer c USING (customer_id)
            WHERE c.customer_id IS NULL
        """).fetchone()[0]
        orphan_categories = con.execute("""
            SELECT COUNT(*) FROM fct_orders f
            LEFT JOIN dim_product_category p USING (product_category)
            WHERE p.product_category IS NULL
        """).fetchone()[0]
        add(
            "tidak ada foreign key yatim (customer, kategori)",
            orphan_customers == 0 and orphan_categories == 0,
            f"customer={orphan_customers}, kategori={orphan_categories}",
        )

        mart_net = float(con.execute(
            "SELECT COALESCE(SUM(net_sales), 0) FROM mart_daily_sales"
        ).fetchone()[0])
        fct_net = fct.loc[
            fct["status"].isin(["paid", "shipped"]), "net_amount"
        ].sum()
        add(
            "total mart_daily_sales = net_amount paid+shipped di fct",
            abs(mart_net - fct_net) < 1e-6,
            f"{mart_net:.2f} vs {fct_net:.2f}",
        )

    finally:
        con.close()

    return checks


def main():
    checks = run_checks(SOURCE, RAW, VALID, REJECTED, WAREHOUSE)

    print("=== AUDIT PIPELINE ===")
    failed = 0

    for name, ok, detail in checks:
        print(f"[{'PASS' if ok else 'FAIL'}] {name}  ({detail})")
        failed += 0 if ok else 1

    print(f"\nRingkasan: {len(checks) - failed}/{len(checks)} cek lulus")

    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
