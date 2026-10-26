"""Benchmark Pandas vs Polars (eager dan lazy) dengan pekerjaan yang sama.

Pekerjaan: baca Parquet -> parse order_ts -> gross/net amount -> total
net_amount per (product_category, order_date). Kunci order_date membuat parse
order_ts benar-benar dibutuhkan hasil, sehingga optimizer lazy Polars tidak
dapat membuangnya (agregasi per kategori saja membuat parse dianggap tak
terpakai dan menggelembungkan keunggulan lazy). Tiap implementasi diukur setelah warm-up,
diulang REPEATS kali, dan hasil utama adalah median. Hasil ketiga
implementasi diperiksa setara sebelum waktu dilaporkan.
"""

import argparse
import platform
import statistics
import tempfile
from pathlib import Path
from time import perf_counter

import pandas as pd
import polars as pl

INPUT_PATH = Path("data/valid/orders_valid.parquet")
RESULT_PATH = Path("data/benchmark/benchmark_results.csv")
SIZES = [30, 10_000, 100_000, 1_000_000]


def run_pandas(path):
    df = pd.read_parquet(path)

    df["order_ts"] = pd.to_datetime(df["order_ts"], utc=True, errors="raise")
    df["gross_amount"] = df["quantity"] * df["unit_price"]
    df["net_amount"] = df["gross_amount"] * (1 - df["discount_pct"])

    df["order_date"] = df["order_ts"].dt.strftime("%Y-%m-%d")

    return (
        df.groupby(["product_category", "order_date"])["net_amount"]
        .sum()
        .reset_index()
    )


def run_polars_eager(path):
    df = pl.read_parquet(path)

    df = df.with_columns(
        pl.col("order_ts").str.to_datetime(time_zone="UTC", strict=True),
        (pl.col("quantity") * pl.col("unit_price")).alias("gross_amount"),
    ).with_columns(
        (pl.col("gross_amount") * (1 - pl.col("discount_pct"))).alias("net_amount")
    )

    return (
        df.with_columns(pl.col("order_ts").dt.strftime("%Y-%m-%d").alias("order_date"))
        .group_by(["product_category", "order_date"])
        .agg(pl.col("net_amount").sum())
        .to_pandas()
    )


def run_polars_lazy(path):
    return (
        pl.scan_parquet(path)
        .with_columns(
            pl.col("order_ts").str.to_datetime(time_zone="UTC", strict=True),
            (pl.col("quantity") * pl.col("unit_price")).alias("gross_amount"),
        )
        .with_columns(
            (pl.col("gross_amount") * (1 - pl.col("discount_pct"))).alias("net_amount")
        )
        .with_columns(pl.col("order_ts").dt.strftime("%Y-%m-%d").alias("order_date"))
        .group_by(["product_category", "order_date"])
        .agg(pl.col("net_amount").sum())
        .collect()
        .to_pandas()
    )


IMPLEMENTATIONS = {
    "pandas": run_pandas,
    "polars_eager": run_polars_eager,
    "polars_lazy": run_polars_lazy,
}


KEYS = ["product_category", "order_date"]


def normalize(result):
    return result.sort_values(KEYS).reset_index(drop=True)


def results_equivalent(a, b, tolerance=1e-6):
    """Setara bila kunci grup sama dan net_amount berbeda < tolerance (relatif)."""
    a, b = normalize(a), normalize(b)

    if not a[KEYS].equals(b[KEYS]):
        return False

    return all(
        abs(x - y) <= tolerance * max(1.0, abs(x))
        for x, y in zip(a["net_amount"], b["net_amount"], strict=True)
    )


def make_dataset(base_path, rows, out_path):
    """Replikasi data valid hingga `rows` baris dengan order_id unik."""
    base = pd.read_parquet(base_path)
    repeats = -(-rows // len(base))
    big = pd.concat([base] * repeats, ignore_index=True).head(rows)
    big["order_id"] = [f"B{i:08d}" for i in range(len(big))]
    big.to_parquet(out_path, index=False)

    return out_path


def time_it(function, path, repeats, warmups):
    for _ in range(warmups):
        function(path)

    timings = []
    result = None

    for _ in range(repeats):
        start = perf_counter()
        result = function(path)
        timings.append(perf_counter() - start)

    return timings, result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repeats", type=int, default=7)
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument("--sizes", type=int, nargs="+", default=SIZES)
    parser.add_argument("--input", default=str(INPUT_PATH))
    parser.add_argument("--out", default=str(RESULT_PATH))
    args = parser.parse_args()

    base_path = Path(args.input)

    if not base_path.exists():
        raise FileNotFoundError(f"File tidak ditemukan: {base_path}")

    print("=== LINGKUNGAN ===")
    print("Python  :", platform.python_version())
    print("Pandas  :", pd.__version__)
    print("Polars  :", pl.__version__)
    print("OS/CPU  :", platform.platform(), "|", platform.processor())
    print(f"Metode  : {args.warmups} warm-up, {args.repeats} pengulangan, median")

    rows_out = []

    with tempfile.TemporaryDirectory() as tmp:
        for size in args.sizes:
            path = (
                base_path
                if size == len(pd.read_parquet(base_path))
                else make_dataset(base_path, size, Path(tmp) / f"orders_{size}.parquet")
            )

            outputs = {}
            print(f"\n--- {size:,} baris ---")

            for name, function in IMPLEMENTATIONS.items():
                timings, result = time_it(function, path, args.repeats, args.warmups)
                outputs[name] = result
                median = statistics.median(timings)

                rows_out.append({
                    "rows": size,
                    "implementation": name,
                    "median_s": round(median, 6),
                    "min_s": round(min(timings), 6),
                    "max_s": round(max(timings), 6),
                    "repeats": args.repeats,
                })
                print(
                    f"{name:13s} median={median:.6f}s "
                    f"min={min(timings):.6f}s max={max(timings):.6f}s"
                )

            equal = all(
                results_equivalent(outputs["pandas"], other)
                for other in outputs.values()
            )
            print("Hasil ketiga implementasi setara:", equal)

            if not equal:
                raise SystemExit("Hasil implementasi berbeda; benchmark tidak valid.")

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows_out).to_csv(out_path, index=False)

    print("\n=== RINGKASAN (median detik) ===")
    pivot = pd.DataFrame(rows_out).pivot(
        index="rows", columns="implementation", values="median_s"
    )
    print(pivot)
    print(f"\nTabel lengkap: {out_path}")
    print(
        "Catatan: waktu mencakup baca Parquet + parse + hitung + agregasi, "
        "hanya berlaku untuk workload, mesin, dan versi di atas."
    )


if __name__ == "__main__":
    main()
