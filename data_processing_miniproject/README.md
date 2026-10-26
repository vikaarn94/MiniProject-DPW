# End-to-End Event-to-Warehouse Pipeline: Analisis Transaksi E-Commerce

Mini project praktikum **Data Processing Workflow** (D4 Sains Data Terapan, PENS).
Pipeline ini membaca transaksi e-commerce, memvalidasi dengan data contract, mengalirkan event lewat Kafka,
mentransformasi, lalu membentuk data warehouse di DuckDB yang dikelola dbt.

**Penulis:** Syavikarani Cahya Putri (NRP 3324600054, Sains Data Terapan B)

## 1. Arsitektur singkat

```
JALUR BATCH
orders.csv -> batch_ingest.py -> data/raw/orders.parquet -> validate.py (Pydantic)
           -> data/valid/orders_valid.parquet (30) + data/rejected/orders_rejected.json (6)
           -> transform_data.py -> data/warehouse/orders_transformed.parquet
           -> build_warehouse.py (DuckDB)  dan  dbt (stg -> dim -> fct -> mart)

JALUR STREAMING (terpisah)
producer.py -> Kafka (topic orders, 3 partition) -> consumer.py (Pydantic)
            -> data/stream/valid.jsonl + data/stream/rejected.jsonl
```

Diagram lengkap: `pipeline_architecture.png`; model dimensional: `star_schema.png`.

## 2. Struktur folder

```
data_processing_miniproject/
├── data/
│   ├── orders.csv              # dataset sumber (36 baris, sebagian sengaja tidak valid)
│   ├── raw/                    # orders.parquet + watermark.json (hasil ingestion)
│   ├── valid/ rejected/        # hasil validasi batch
│   ├── stream/                 # hasil consumer Kafka (jsonl)
│   └── warehouse/              # parquet transformasi, DuckDB
├── scripts/
│   ├── check_data.py  batch_ingest.py  check_parquet.py
│   ├── validate.py    producer.py      consumer.py      test_duplicate_event.py
│   ├── transform_data.py  benchmark.py  data_skew.py
│   └── build_warehouse.py  check_warehouse.py
├── dbt_project/                # dbt_project.yml, profiles.yml, models/staging, models/marts
├── docker-compose.yml          # Kafka (apache/kafka:3.9.0, port 9092)
├── requirements.txt
└── README.md
```

## 3. Prasyarat

- Python 3.10+ (diuji dengan 3.10.0)
- Docker dan Docker Compose (diuji: Docker 29.5.2, Compose v5.1.3)
- Windows PowerShell (perintah di bawah memakai sintaks PowerShell; di Linux/macOS aktifkan venv dengan `source .venv/bin/activate`)

## 4. Instalasi

Jalankan dari folder utama project:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
python -c "import pandas, pyarrow, pydantic, polars, duckdb; print('Library utama berhasil diimpor')"
```

Buat folder keluaran bila belum ada: `data/raw`, `data/rejected`, `data/valid`, `data/stream`, `data/warehouse`.

## 5. Menjalankan pipeline (urut)

| # | Tahap | Perintah | Hasil yang diharapkan |
|---|-------|----------|-----------------------|
| 1 | Periksa dataset | `python scripts/check_data.py` | 36 baris, 9 kolom; 1 nilai kosong di `customer_id`, 1 di `order_ts`; status: paid 25, shipped 8, cancelled 1, refunded 1, pending 1 |
| 2 | Batch ingestion | `python scripts/batch_ingest.py --source data/orders.csv --out data/raw --page-size 10` | Page 1-3 = 10 record, Page 4 = 6 record; total unik 36; `data/raw/orders.parquet` dan `watermark.json` terbentuk |
| 3 | Uji jalan ulang | jalankan perintah tahap 2 sekali lagi | Total unik tetap 36 (tidak ada duplikasi) |
| 4 | Periksa Parquet | `python scripts/check_parquet.py` | 36 record, tipe data terjaga |
| 5 | Validasi (data contract) | `python scripts/validate.py` | Valid 30, Rejected 6; `data/valid/orders_valid.parquet` dan `data/rejected/orders_rejected.json` |
| 6 | Kafka | `docker compose up -d` lalu `docker compose ps` | container `dpw-kafka` berstatus healthy |
| 7 | Consumer (terminal 1) | `python scripts/consumer.py` | menunggu pesan, menulis `data/stream/valid.jsonl` |
| 8 | Producer (terminal 2) | `python scripts/producer.py` | 20 event terkirim dengan partition dan offset |
| 9 | Uji duplikasi event | `python scripts/test_duplicate_event.py` | 2 event `TEST-DUPLICATE-001` keduanya diterima consumer (lihat Keterbatasan) |
| 10 | Transformasi | `python scripts/transform_data.py` | 30 baris; `gross_amount = quantity x unit_price`, `net_amount = gross_amount x (1 - discount_pct)` |
| 11 | Benchmark Pandas vs Polars | `python scripts/benchmark.py` | waktu dan hasil agregasi kedua library dicetak |
| 12 | Eksperimen data skew | `python scripts/data_skew.py` | 170 baris, 25 customer; waktu groupby normal vs skewed |
| 13 | Warehouse DuckDB | `python scripts/build_warehouse.py` lalu `python scripts/check_warehouse.py` | stg 30, dim_customer 25, dim_product_category 4, fct 30, mart_daily_sales 28 baris |

### dbt

```powershell
dbt debug --project-dir dbt_project --profiles-dir dbt_project
dbt run   --project-dir dbt_project --profiles-dir dbt_project     # PASS=5
dbt test  --project-dir dbt_project --profiles-dir dbt_project     # PASS=10
dbt docs generate --project-dir dbt_project --profiles-dir dbt_project
dbt docs serve    --project-dir dbt_project --profiles-dir dbt_project   # http://localhost:8080
```

Model dbt: `stg_orders` (view) -> `dim_customer`, `dim_product_category` -> `fct_orders` -> `mart_daily_sales` (table).

## 6. Data contract (ringkas)

| Field | Aturan |
|-------|--------|
| order_id, customer_id, product_category, payment_method | wajib, tidak kosong |
| order_ts | datetime valid |
| quantity | integer > 0 |
| unit_price | >= 0 |
| discount_pct | 0 sampai 1 |
| status | `paid`, `shipped`, `cancelled`, `refunded` |
| field tambahan | ditolak (`extra="forbid"`) |

Record yang melanggar aturan dipisahkan ke file rejected beserta alasan error Pydantic.

## 7. Keterbatasan yang diketahui

- Sumber ingestion adalah CSV lokal yang dibaca per halaman (simulasi REST API); tidak ada permintaan HTTP ke API eksternal.
- Retry dengan backoff ada di `get_page`, tetapi belum ada simulasi error pada mini project ini.
- Kafka berjalan sebagai satu broker (replication factor 1); cocok untuk praktikum, bukan produksi.
- Consumer hanya memvalidasi; event dengan `order_id` sama tetap tertulis dua kali. Idempotensi (kunci unik `order_id` di sink) belum diimplementasikan di jalur streaming.
- Jalur streaming belum terhubung ke warehouse; warehouse dibangun dari jalur batch.
- Benchmark dan eksperimen skew memakai data kecil dan sekali ukur, sehingga hasilnya tidak untuk digeneralisasi.
- Model `marts` memakai materialisasi `table` (full refresh), belum `incremental`.

## 8. Pengembangan lanjutan

Idempotensi di sink streaming, penghubung jalur streaming ke warehouse, model dbt incremental,
otomatisasi pipeline (orkestrasi), dan optimasi untuk data skala besar.
