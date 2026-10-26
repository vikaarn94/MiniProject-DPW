
from pathlib import Path
import pandas as pd

INPUT_PATH = Path("data/valid/orders_valid.parquet")
OUTPUT_PATH = Path("data/warehouse/orders_transformed.parquet")


def main():
    if not INPUT_PATH.exists():
        raise FileNotFoundError(
            f"File tidak ditemukan: {INPUT_PATH}. "
            "Jalankan validasi data terlebih dahulu."
        )

    df = pd.read_parquet(INPUT_PATH)

    # Normalisasi tipe data
    df["order_ts"] = pd.to_datetime(
        df["order_ts"], utc=True, errors="raise"
    )
    df["quantity"] = pd.to_numeric(df["quantity"]).astype("int64")
    df["unit_price"] = pd.to_numeric(df["unit_price"])
    df["discount_pct"] = pd.to_numeric(df["discount_pct"])

    # Perhitungan nilai transaksi
    df["gross_amount"] = df["quantity"] * df["unit_price"]
    df["net_amount"] = (
        df["gross_amount"] * (1 - df["discount_pct"])
    )

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUTPUT_PATH, index=False)

    print("Transformasi selesai.")
    print(f"Jumlah data: {len(df)}")
    print(f"File output: {OUTPUT_PATH}")
    print(df[[
        "order_id", "gross_amount", "net_amount"
    ]].head())


if __name__ == "__main__":
    main()
