"""Eksperimen data skew pada group_by.

Bagian 1 (sesuai tugas): gandakan baris satu customer dominan pada data asli.
Bagian 2 (terkontrol): bandingkan data seragam vs skewed dengan JUMLAH BARIS
SAMA, supaya selisih waktu tidak bercampur dengan efek ukuran data. Setiap
pengukuran diulang dan dilaporkan sebagai median.
"""

import argparse
import statistics
from pathlib import Path
from time import perf_counter

import numpy as np
import pandas as pd
import polars as pl

INPUT_PATH = Path("data/valid/orders_valid.parquet")
OUTPUT_PATH = Path("data/warehouse/orders_skewed.parquet")
RESULT_PATH = Path("data/benchmark/skew_results.csv")


def add_amounts(df):
    df = df.copy()
    df["gross_amount"] = df["quantity"] * df["unit_price"]
    df["net_amount"] = df["gross_amount"] * (1 - df["discount_pct"])
    return df


def distribution(df):
    counts = df["customer_id"].value_counts()
    return {
        "rows": len(df),
        "customers": int(counts.size),
        "dominant_customer": counts.index[0],
        "dominant_rows": int(counts.iloc[0]),
        "dominant_share": round(float(counts.iloc[0] / len(df)), 4),
    }


def groupby_pandas(df):
    return df.groupby("customer_id").agg(
        total_orders=("order_id", "count"),
        total_sales=("net_amount", "sum"),
    )


def groupby_polars(df):
    return df.group_by("customer_id").agg(
        pl.len().alias("total_orders"),
        pl.col("net_amount").sum().alias("total_sales"),
    )


def median_time(function, data, repeats, warmups=2):
    for _ in range(warmups):
        function(data)

    timings = []

    for _ in range(repeats):
        start = perf_counter()
        function(data)
        timings.append(perf_counter() - start)

    return statistics.median(timings)


def make_synthetic(rows, customers, dominant_share, seed=42):
    """`rows` baris; dominant_share dari baris milik satu customer (0 = seragam)."""
    rng = np.random.default_rng(seed)
    ids = rng.integers(0, customers, size=rows)

    if dominant_share > 0:
        ids[rng.random(rows) < dominant_share] = 0

    return pd.DataFrame({
        "order_id": [f"S{i:09d}" for i in range(rows)],
        "customer_id": [f"C{i:06d}" for i in ids],
        "net_amount": rng.random(rows) * 100,
    })


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repeats", type=int, default=7)
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--customers", type=int, default=10_000)
    parser.add_argument("--out", default=str(RESULT_PATH))
    args = parser.parse_args()

    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"File tidak ditemukan: {INPUT_PATH}")

    results = []

    # ---- Bagian 1: eksperimen pada data asli (ukuran berbeda!) ----
    df = add_amounts(pd.read_parquet(INPUT_PATH))
    dominant = df["customer_id"].iloc[0]
    skewed = pd.concat(
        [df, df[df["customer_id"] == dominant]] * 5, ignore_index=True
    )

    print("=== BAGIAN 1: data asli vs penggandaan customer dominan ===")
    for label, frame in [("normal", df), ("skewed_dup", skewed)]:
        info = distribution(frame)
        seconds = median_time(groupby_pandas, frame, args.repeats)
        print(f"{label:11s} {info} | pandas median={seconds:.6f}s")
        results.append({"experiment": "dup_original", "label": label, **info,
                        "engine": "pandas", "median_s": round(seconds, 6)})

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    skewed.to_parquet(OUTPUT_PATH, index=False)
    print("Catatan: jumlah baris berbeda (", len(df), "vs", len(skewed),
          "), jadi selisih waktu belum murni efek skew.")

    # ---- Bagian 2: baris sama, distribusi berbeda ----
    print(f"\n=== BAGIAN 2: {args.rows:,} baris, distribusi seragam vs skewed ===")
    for share in [0.0, 0.5, 0.9]:
        frame = make_synthetic(args.rows, args.customers, share)
        info = distribution(frame)
        label = "uniform" if share == 0 else f"skew_{int(share * 100)}pct"
        polars_frame = pl.from_pandas(frame)

        for engine, function, data in [
            ("pandas", groupby_pandas, frame),
            ("polars", groupby_polars, polars_frame),
        ]:
            seconds = median_time(function, data, args.repeats)
            print(f"{label:10s} {engine:6s} dominant_share={info['dominant_share']:.2f} "
                  f"median={seconds:.6f}s")
            results.append({"experiment": "equal_rows", "label": label, **info,
                            "engine": engine, "median_s": round(seconds, 6)})

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(results).to_csv(out_path, index=False)

    print(f"\nTabel lengkap: {out_path}")
    print("Dataset skewed (bagian 1) disimpan di:", OUTPUT_PATH)
    print("Kesimpulan harus mengikuti angka di atas; tidak ada uji OOM.")


if __name__ == "__main__":
    main()
