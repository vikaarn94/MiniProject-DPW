## Mini Project: End-to-End Event-to-Warehouse Pipeline

Berikut contoh implementasi yang dapat dijadikan acuan.
Boleh menggunakan struktur berbeda selama semua kompetensi yang diminta dibuktikan.

## 1. Persiapan lingkungan

### Langkah 1 — Buat virtual environment

```bash
python -m venv .venv
# Windows PowerShell
.venv\Scripts\Activate.ps1
# macOS/Linux
source .venv/bin/activate
pip install -r requirements.txt
```

Pastikan Python dan Docker Desktop/Engine tersedia.

### Langkah 2 — Periksa dataset

Buka `data/orders.csv`. Dataset sengaja memuat beberapa record cacat untuk menguji kontrak data. Jangan menghapus record cacat sebelum proses validasi, karena record tersebut merupakan bagian dari tugas.

### Langkah 3 — Jalankan ekstraksi batch simulasi API

```bash
python scripts/batch_ingest.py --source data/orders.csv --out data/raw
```

Skrip membaca CSV per halaman, menambahkan retry sederhana yang dapat diperluas, menerapkan watermark, lalu menyimpan hasil ke Parquet. Untuk implementasi produksi, adapter CSV perlu diganti dengan `requests.get()` ke endpoint REST API, menangani status HTTP seperti 429/5xx, `Retry-After`, timeout, autentikasi, pagination sesuai API, dan checkpoint yang disimpan secara persisten.

**Bukti yang dikumpulkan:** log halaman yang dibaca, jumlah record, file Parquet, dan demonstrasi menjalankan ulang tanpa menambah ID yang sudah diproses.

## 2. Data contract dan fail-fast

### Langkah 4 — Definisikan skema

Gunakan model Pydantic dengan field dan aturan berikut:
- `order_id`: string tidak kosong.
- `customer_id`: string tidak kosong.
- `order_ts`: timestamp yang dapat diparse.
- `quantity`: integer lebih besar dari nol.
- `unit_price`: angka >= 0.
- `discount_pct`: angka dalam rentang 0–1.
- `status`: salah satu dari `paid`, `shipped`, `cancelled`, `refunded`.

Contoh inti:

```python
from datetime import datetime
from typing import Literal
from pydantic import BaseModel, Field, ConfigDict

class Order(BaseModel):
    model_config = ConfigDict(extra="forbid")
    order_id: str = Field(min_length=1)
    customer_id: str = Field(min_length=1)
    order_ts: datetime
    product_category: str = Field(min_length=1)
    quantity: int = Field(gt=0)
    unit_price: float = Field(ge=0)
    discount_pct: float = Field(ge=0, le=1)
    payment_method: str = Field(min_length=1)
    status: Literal["paid", "shipped", "cancelled", "refunded"]
```

Validasi setiap record secara terpisah. Simpan record yang lolos ke `valid_orders`; simpan record gagal ke `rejected_orders` bersama `order_id` bila tersedia, payload asal, dan pesan kesalahan. Jika kontrak untuk satu batch diwajibkan fail-fast, hentikan pemuatan batch tersebut ketika ditemukan pelanggaran. Alternatif karantina per-record boleh digunakan bila dijelaskan dan diuji.

**Bukti yang dikumpulkan:** jumlah record valid/invalid, contoh error yang dapat ditindaklanjuti, dan bukti bahwa invalid tidak masuk ke tabel warehouse.

## 3. Transformasi dan Parquet

### Langkah 5 — Hitung metrik

Untuk setiap record valid:

```python
gross_amount = quantity * unit_price
net_amount = gross_amount * (1 - discount_pct)
```

Pastikan `discount_pct=0.10` berarti diskon 10%, bukan 0,10 persen. Gunakan tipe numerik yang konsisten dan timestamp yang terstandardisasi.

### Langkah 6 — Simpan format kolumnar

Simpan data mentah yang sudah diekstrak ke Parquet. Simpan hasil validasi/transformasi pada lokasi terpisah agar raw layer tetap dapat diaudit. Parquet sesuai untuk pembacaan kolumnar dan menjaga tipe data lebih baik dibanding CSV, tetapi tidak dengan sendirinya menjamin tidak ada duplikasi.

### Langkah 7 — Uji watermark dan idempotensi

Pola sederhana:
1. Baca watermark terakhir yang tersimpan.
2. Ambil record dengan timestamp/ID yang lebih baru.
3. Gabungkan dengan data tujuan berdasarkan kunci bisnis `order_id`.
4. Deduplicasi/upsert berdasarkan `order_id`.
5. Simpan watermark baru hanya setelah penulisan sukses.

Jangan hanya menyimpan watermark di variabel memori, karena nilainya hilang ketika proses berhenti. Untuk demo, file checkpoint JSON cukup; pada sistem produksi gunakan penyimpanan checkpoint yang andal. Perhatikan bahwa watermark timestamp saja bisa melewatkan record yang datang terlambat dengan timestamp lama. Mitigasi: lookback window dan upsert berdasarkan kunci.

## 4. Kafka streaming

### Langkah 8 — Jalankan broker

```bash
docker compose up -d
docker compose ps
```

Gunakan `docker compose logs` bila broker gagal hidup. Setelah selesai:

```bash
docker compose down
```

### Langkah 9 — Producer dan consumer

Producer membaca beberapa baris sumber dan mengirim JSON ke topic `orders`. Consumer membaca topic, memvalidasi payload dengan kontrak yang sama, lalu menyimpan hasil valid dan rejected.

Checklist implementasi:
1. Producer menetapkan key pesan ke `order_id` bila sesuai kebutuhan partisi.
2. Consumer menangani JSON rusak dan validasi gagal.
3. Consumer mencatat topic, partition, dan offset.
4. Consumer melakukan commit offset setelah proses sink berhasil bila menggunakan manual commit.
5. Sink menerapkan deduplikasi/upsert berdasarkan `order_id`.

### Langkah 10 — jaminan pengiriman

- **At-most-once:** pesan dapat hilang, tetapi tidak diproses ulang; misalnya offset di-commit sebelum pemrosesan selesai.
- **At-least-once:** pesan tidak sengaja dilewatkan, tetapi dapat diproses ulang; misalnya commit dilakukan setelah sink menulis dan proses crash pada waktu yang tidak tepat.
- **Exactly-once:** membutuhkan mekanisme transaksional/idempotensi yang mencakup batas sistem terkait. Kafka transactions dapat membantu dalam alur Kafka-to-Kafka, tetapi tidak otomatis membuat penulisan ke file atau DuckDB exactly-once.


### Langkah 11 — Consumer groups, partitions, offsets

- **Partition** membagi topic menjadi urutan log yang dapat diproses paralel; urutan dijamin di dalam satu partition, bukan antar-partition.
- **Consumer group** membagi partisi kepada anggota group; dalam satu group, satu partisi pada satu waktu ditangani satu consumer aktif.
- **Offset** adalah posisi pesan pada partition dan menjadi dasar pelacakan kemajuan consumer.
- Menambah consumer tidak selalu menambah paralelisme jika jumlah partition lebih sedikit daripada consumer.

## 5. Benchmark Pandas dan Polars

### Langkah 12 — Gunakan dataset identik

Jalankan transformasi yang sama di Pandas dan Polars. Lakukan warm-up jika relevan, ulangi beberapa kali, dan laporkan median atau rata-rata beserta ukuran data dan spesifikasi mesin. Jangan membandingkan pekerjaan yang berbeda atau hanya satu kali run.

Contoh Pandas:

```python
import pandas as pd
df = pd.read_csv("data/orders.csv")
df["gross_amount"] = df["quantity"] * df["unit_price"]
df["net_amount"] = df["gross_amount"] * (1 - df["discount_pct"])
summary = df.groupby("product_category", dropna=False)["net_amount"].sum()
```

Contoh Polars eager:

```python
import polars as pl
df = pl.read_csv("data/orders.csv")
df = df.with_columns(
    (pl.col("quantity") * pl.col("unit_price")).alias("gross_amount"),
    (pl.col("quantity") * pl.col("unit_price") *
     (1 - pl.col("discount_pct"))).alias("net_amount"),
)
summary = df.group_by("product_category").agg(pl.col("net_amount").sum())
```

Contoh lazy:

```python
result = (
    pl.scan_csv("data/orders.csv")
    .with_columns(
        (pl.col("quantity") * pl.col("unit_price")).alias("gross_amount"),
        (pl.col("quantity") * pl.col("unit_price") *
         (1 - pl.col("discount_pct"))).alias("net_amount"),
    )
    .group_by("product_category")
    .agg(pl.col("net_amount").sum())
    .collect()
)
```

Lazy execution memungkinkan Polars mengoptimalkan rencana query sebelum dieksekusi, misalnya projection/filter pushdown. Lazy tidak selalu lebih cepat untuk setiap dataset atau setiap operasi; ukur hasil aktual.

### Langkah 13 — Eksperimen data skew

1. Buat dataset turunan yang mengulang sebagian besar record dengan `customer_id` yang sama.
2. Hitung distribusi jumlah baris per `customer_id`.
3. Bandingkan agregasi normal dan skewed dengan kode, ukuran data, waktu, dan penggunaan memori bila tersedia.
4. Jelaskan bahwa skew membuat sebagian grup jauh lebih besar daripada grup lain, sehingga beban tidak merata pada operasi terdistribusi.
5. Bahas mitigasi: agregasi parsial, salting key bila sesuai, pemrosesan per partisi/batch, memilih kolom yang diperlukan saja, dan menghindari `collect`/materialisasi berlebihan.

Jangan menyebut terjadi OOM jika proses tidak benar-benar gagal karena kehabisan memori.

## 6. DuckDB dan dbt

### Langkah 14 — Siapkan DuckDB

Gunakan DuckDB sebagai database lokal. Muat data valid hasil transformasi ke tabel raw/staging. Pastikan record rejected tidak dimuat. Contoh SQL analitik dasar:

```sql
SELECT
  CAST(order_ts AS DATE) AS order_date,
  product_category,
  SUM(quantity * unit_price * (1 - discount_pct)) AS net_sales
FROM raw_orders
WHERE status IN ('paid', 'shipped')
GROUP BY 1, 2
ORDER BY 1, 2;
```

Sesuaikan definisi penjualan yang diakui dengan kebijakan bisnis. Jika cancelled/refunded harus dikurangkan, definisikan aturan eksplisit dan konsisten.

### Langkah 15 — Buat model dimensional

Model minimal yang disarankan:
- `dim_customer`: satu baris per `customer_id`.
- `dim_product_category`: satu baris per kategori.
- `fct_orders`: satu baris per `order_id`, berisi foreign key dan metrik transaksi.
- `mart_daily_sales`: agregasi tanggal/kategori/metode pembayaran.

Ini merupakan pendekatan star schema bila fact terhubung langsung ke dimensi yang relatif denormalisasi. Snowflake schema memecah dimensi ke tabel terkait yang lebih ternormalisasi. Pilih berdasarkan kebutuhan analitik, pemeliharaan, dan kompleksitas join.

### Langkah 16 — Buat tes dbt

Contoh `schema.yml`:

```yaml
version: 2

models:
  - name: fct_orders
    columns:
      - name: order_id
        data_tests:
          - unique
          - not_null
      - name: customer_id
        data_tests:
          - not_null
          - relationships:
              arguments:
                to: ref('dim_customer')
                field: customer_id
```

Sintaks tepat dapat berbeda menurut versi dbt. Pada versi yang menggunakan sintaks lama, `tests:` dapat digunakan menggantikan `data_tests:`. Jalankan `dbt --version` dan sesuaikan dokumentasi versi terpasang.

### Langkah 17 — Jalankan dbt

Di direktori proyek dbt:

```bash
dbt debug
dbt run
dbt test
dbt docs generate
dbt docs serve
```

Sertakan hasil `dbt run`, `dbt test`, dan dokumentasi. Jika menggunakan DuckDB, instal adapter `dbt-duckdb` dan konfigurasi profile sesuai petunjuk versi yang terpasang.

**Incremental model** memproses data baru/perubahan saja, bukan membangun ulang seluruh tabel setiap kali. Gunakan kunci unik, filter watermark/lookback yang benar, dan strategi merge/upsert yang didukung adapter.

## 7. Pertanyaan analitik dan cara menjawab

Gunakan tabel mart atau query SQL untuk menjawab:

```sql
-- Penjualan bersih per hari
SELECT order_date, SUM(net_sales) AS net_sales
FROM mart_daily_sales
GROUP BY order_date
ORDER BY order_date;

-- Kategori dengan penjualan tertinggi
SELECT product_category, SUM(net_sales) AS net_sales
FROM mart_daily_sales
GROUP BY product_category
ORDER BY net_sales DESC;

-- Jumlah pesanan per status
SELECT status, COUNT(*) AS order_count
FROM fct_orders
GROUP BY status
ORDER BY order_count DESC;

-- Nilai pesanan yang ditolak
-- Hitung dari tabel/file rejected dan kelompokkan berdasarkan error reason.
```

Angka final bergantung pada record yang lolos kontrak dan aturan bisnis yang dipilih. Karena itu, mahasiswa harus menyertakan query serta hasil aktual, bukan angka yang diperkirakan.

## 8. Struktur folder hasil akhir yang disarankan

```text
submission/
├── README.md
├── requirements.txt
├── docker-compose.yml
├── data/
│   ├── source/
│   ├── raw/
│   ├── valid/
│   └── rejected/
├── scripts/
│   ├── batch_ingest.py
│   ├── validate.py
│   ├── producer.py
│   ├── consumer.py
│   └── benchmark.py
├── warehouse/
│   └── warehouse.duckdb
├── dbt_project/
│   ├── dbt_project.yml
│   ├── models/
│   └── tests/
└── report.md
```

