
from pathlib import Path
from time import perf_counter

import pandas as pd

INPUT_PATH = Path("data/valid/orders_valid.parquet")
OUTPUT_PATH = Path("data/warehouse/orders_skewed.parquet")


def measure_groupby(df, label):
    start = perf_counter()

    result = (
        df.groupby("customer_id")
        .agg(
            total_orders=("order_id", "count"),
            total_sales=("net_amount", "sum"),
        )
    )

    elapsed = perf_counter() - start

    print(f"\n{label}")
    print(f"Jumlah baris: {len(df)}")
    print(f"Jumlah customer: {df['customer_id'].nunique()}")
    print(f"Waktu groupby: {elapsed:.6f} detik")
    print(result.sort_values(
        "total_orders", ascending=False
    ).head())

    return elapsed


def main():
    if not INPUT_PATH.exists():
        raise FileNotFoundError(
            f"File tidak ditemukan: {INPUT_PATH}"
        )

    df = pd.read_parquet(INPUT_PATH)

    df["gross_amount"] = df["quantity"] * df["unit_price"]
    df["net_amount"] = (
        df["gross_amount"] * (1 - df["discount_pct"])
    )

    # Dataset normal
    normal_time = measure_groupby(df, "DATA NORMAL")

    # Gandakan sebagian data untuk satu customer dominan
    if df.empty:
        print("Dataset kosong; eksperimen dihentikan.")
        return

    dominant_customer = df["customer_id"].iloc[0]
    repeated_rows = df[
        df["customer_id"] == dominant_customer
    ]

    if repeated_rows.empty:
        print("Tidak ada baris untuk customer dominan.")
        return

    skewed = pd.concat(
        [df, repeated_rows] * 5,
        ignore_index=True,
    )

    skew_time = measure_groupby(skewed, "DATA SKEWED")

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    skewed.to_parquet(OUTPUT_PATH, index=False)

    print("\n=== PERBANDINGAN ===")
    print(f"Normal : {normal_time:.6f} detik")
    print(f"Skewed : {skew_time:.6f} detik")
    print(f"Dataset skewed disimpan di: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
