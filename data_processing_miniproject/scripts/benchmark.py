
from pathlib import Path
from time import perf_counter

import pandas as pd
import polars as pl

INPUT_PATH = "data/valid/orders_valid.parquet"


def run_pandas():
    df = pd.read_parquet(INPUT_PATH)

    df["order_ts"] = pd.to_datetime(
        df["order_ts"], utc=True, errors="raise"
    )
    df["gross_amount"] = df["quantity"] * df["unit_price"]
    df["net_amount"] = (
        df["gross_amount"] * (1 - df["discount_pct"])
    )

    # Agregasi agar kedua library melakukan pekerjaan serupa
    result = df.groupby("product_category")["net_amount"].sum()
    return len(df), result


def run_polars():
    result = (
        pl.scan_parquet(INPUT_PATH)
        .with_columns(
            pl.col("order_ts").str.to_datetime(
                time_zone="UTC", strict=True
            ),
            (
                pl.col("quantity") * pl.col("unit_price")
            ).alias("gross_amount"),
        )
        .with_columns(
            (
                pl.col("gross_amount")
                * (1 - pl.col("discount_pct"))
            ).alias("net_amount")
        )
        .group_by("product_category")
        .agg(pl.col("net_amount").sum())
        .collect()
    )

    # Hitung jumlah baris sumber untuk pelaporan
    row_count = pl.scan_parquet(INPUT_PATH).select(
        pl.len()
    ).collect().item()

    return row_count, result


def measure(label, function):
    start = perf_counter()
    row_count, result = function()
    elapsed = perf_counter() - start

    print(f"\n{label}")
    print(f"Jumlah baris: {row_count}")
    print(f"Waktu: {elapsed:.6f} detik")
    print("Contoh hasil:")
    print(result)

    return elapsed


def main():
    if not Path(INPUT_PATH).exists():
        raise FileNotFoundError(
            f"File tidak ditemukan: {INPUT_PATH}"
        )

    pandas_time = measure("PANDAS", run_pandas)
    polars_time = measure("POLARS LAZY", run_polars)

    print("\n=== RINGKASAN BENCHMARK ===")
    print(f"Pandas       : {pandas_time:.6f} detik")
    print(f"Polars Lazy  : {polars_time:.6f} detik")
    print("Gunakan hasil pengukuran aktual untuk laporan.")


if __name__ == "__main__":
    main()
