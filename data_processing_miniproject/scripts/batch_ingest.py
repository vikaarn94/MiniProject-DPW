
import argparse
import json
import time
from pathlib import Path

import pandas as pd


def get_page(df, page, page_size, retries=3):
    """Membaca satu halaman data dari CSV lokal."""

    for attempt in range(retries):
        try:
            start = page * page_size
            end = start + page_size

            return df.iloc[start:end].copy()

        except Exception:
            if attempt == retries - 1:
                raise

            wait_time = 2 ** attempt
            print(f"Retry dalam {wait_time} detik...")
            time.sleep(wait_time)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--source",
        default="data/orders.csv"
    )

    parser.add_argument(
        "--out",
        default="data/raw"
    )

    parser.add_argument(
        "--page-size",
        type=int,
        default=10
    )

    parser.add_argument(
        "--checkpoint",
        default="data/watermark.json"
    )

    args = parser.parse_args()

    source_path = Path(args.source)
    output_dir = Path(args.out)
    checkpoint_path = Path(args.checkpoint)

    if not source_path.exists():
        raise FileNotFoundError(
            f"File sumber tidak ditemukan: {source_path}"
        )

    if args.page_size <= 0:
        raise ValueError("page-size harus lebih besar dari 0")

    df = pd.read_csv(source_path)

    # Normalisasi timestamp untuk watermark.
    df["order_ts"] = pd.to_datetime(
        df["order_ts"],
        errors="coerce",
        utc=True
    )

    output_dir.mkdir(parents=True, exist_ok=True)

    target_path = output_dir / "orders.parquet"

    # Baca checkpoint sebelumnya.
    last_watermark = None

    if checkpoint_path.exists():
        checkpoint = json.loads(
            checkpoint_path.read_text(encoding="utf-8")
        )
        last_watermark = checkpoint.get("watermark")

    pages = []
    page = 0

    while True:
        chunk = get_page(
            df,
            page,
            args.page_size
        )

        if chunk.empty:
            break

        print(
            f"Page {page + 1}: "
            f"{len(chunk)} record"
        )

        pages.append(chunk)
        page += 1

    if pages:
        extracted = pd.concat(
            pages,
            ignore_index=True
        )
    else:
        extracted = df.iloc[0:0].copy()

    # Filter incremental berdasarkan watermark.
    # Timestamp invalid tetap disimpan untuk audit
    # pada proses validasi berikutnya.
    if last_watermark:
        watermark_ts = pd.Timestamp(last_watermark)

        new_rows = extracted[
            extracted["order_ts"].isna()
            | (extracted["order_ts"] > watermark_ts)
        ].copy()
    else:
        new_rows = extracted.copy()

    # Gabungkan dengan data Parquet sebelumnya.
    if target_path.exists():
        old_rows = pd.read_parquet(target_path)

        combined = pd.concat(
            [old_rows, new_rows],
            ignore_index=True
        )
    else:
        combined = new_rows.copy()

    # Deduplikasi berdasarkan ID pesanan.
    combined = combined.drop_duplicates(
        subset=["order_id"],
        keep="last"
    )

    # Tulis hasil terlebih dahulu.
    combined.to_parquet(
        target_path,
        index=False
    )

    # Simpan watermark hanya setelah penulisan sukses.
    valid_timestamps = combined["order_ts"].dropna()

    if not valid_timestamps.empty:
        new_watermark = valid_timestamps.max().isoformat()

        checkpoint_path.parent.mkdir(
            parents=True,
            exist_ok=True
        )

        checkpoint_path.write_text(
            json.dumps(
                {"watermark": new_watermark},
                indent=2
            ),
            encoding="utf-8"
        )

    print("\n=== HASIL BATCH INGESTION ===")
    print("Total sumber :", len(df))
    print("Record baru  :", len(new_rows))
    print("Total unik   :", len(combined))
    print("Output       :", target_path)
    print("Watermark    :", new_watermark if not valid_timestamps.empty else "Belum tersedia")


if __name__ == "__main__":
    main()
