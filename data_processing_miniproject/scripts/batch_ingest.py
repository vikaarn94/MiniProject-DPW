import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from retry_utils import TransientSourceError, with_retry


class CsvPageSource:
    """Sumber halaman dari CSV lokal yang mensimulasikan REST API berpagination.

    simulate_failures=N membuat N panggilan fetch pertama melempar
    TransientSourceError (setara HTTP 503/timeout) agar retry bisa dibuktikan.
    """

    def __init__(self, df, simulate_failures=0):
        self.df = df
        self.remaining_failures = simulate_failures

    def fetch(self, page, page_size):
        if self.remaining_failures > 0:
            self.remaining_failures -= 1
            raise TransientSourceError("503 Service Unavailable (simulasi)")

        start = page * page_size
        return self.df.iloc[start:start + page_size].copy()


def get_page(source, page, page_size, max_attempts=4, base_delay=1.0, sleep=None):
    """Ambil satu halaman dengan retry + exponential backoff."""
    kwargs = {"max_attempts": max_attempts, "base_delay": base_delay}

    if sleep is not None:
        kwargs["sleep"] = sleep

    return with_retry(lambda: source.fetch(page, page_size), **kwargs)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--source", default="data/orders.csv")
    parser.add_argument("--out", default="data/raw")
    parser.add_argument("--page-size", type=int, default=10)
    parser.add_argument("--checkpoint", default="data/watermark.json")
    parser.add_argument(
        "--simulate-failures",
        type=int,
        default=0,
        help="jumlah kegagalan sementara yang disimulasikan sebelum sukses",
    )
    parser.add_argument("--max-attempts", type=int, default=4)
    parser.add_argument("--base-delay", type=float, default=1.0)
    parser.add_argument(
        "--log",
        default="data/raw/ingestion_log.jsonl",
        help="file JSON-lines berisi ringkasan tiap eksekusi",
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

    source = CsvPageSource(df, simulate_failures=args.simulate_failures)

    pages = []
    page = 0

    while True:
        print(f"Mengambil halaman {page + 1}...")

        chunk = get_page(
            source,
            page,
            args.page_size,
            max_attempts=args.max_attempts,
            base_delay=args.base_delay,
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
        old_rows = None
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
    new_watermark = None

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

    total_before = 0 if old_rows is None else len(old_rows)
    added = len(combined) - total_before

    summary = {
        "run_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "pages": page,
        "read": len(extracted),
        "passed_watermark_filter": len(new_rows),
        "added": added,
        "total_before": total_before,
        "total_unique": len(combined),
        "watermark_before": last_watermark,
        "watermark_after": new_watermark,
    }

    log_path = Path(args.log)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    with log_path.open("a", encoding="utf-8") as log_file:
        log_file.write(json.dumps(summary) + "\n")

    print("\n=== HASIL BATCH INGESTION ===")
    print("Halaman dibaca       :", page)
    print("Record dibaca        :", len(extracted))
    print("Lolos filter watermark:", len(new_rows))
    print("Record ditambahkan   :", added)
    print("Total unik sebelum   :", total_before)
    print("Total unik sesudah   :", len(combined))
    print("Output               :", target_path)
    print("Watermark sebelum    :", last_watermark or "-")
    print("Watermark sesudah    :", new_watermark or "Belum tersedia")
    print("Log eksekusi         :", log_path)


if __name__ == "__main__":
    main()
