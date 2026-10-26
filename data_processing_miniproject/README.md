# Supporting files — Mini Project Data Processing Workflow

## Isi paket
- `data/orders.csv`: data transaksi sintetis, termasuk beberapa baris invalid untuk menguji data contract.
- `scripts/batch_ingest.py`: contoh awal batch pagination, Parquet, checkpoint watermark, dan deduplikasi.
- `requirements.txt`: dependensi Python awal.
- `docker-compose.yml`: konfigurasi single-node Kafka untuk praktikum.
- `MINI_PROJECT.md`: brief tugas mahasiswa.
- `SOLUTION_STEP_BY_STEP.md`: panduan solusi langkah demi langkah.

## Mulai cepat
1. Buat environment Python dan install `requirements.txt`.
2. Jalankan batch ingestion:
   `python scripts/batch_ingest.py --source data/orders.csv --out data/raw`
3. Jalankan Kafka:
   `docker compose up -d`
4. Lengkapi producer, consumer, validasi Pydantic, benchmark, pemuatan DuckDB, dan model dbt sesuai brief.

## Catatan penting
Skrip batch merupakan kerangka edukatif, bukan pipeline produksi: sumbernya CSV lokal yang dipaginasi, bukan API eksternal. Perluas retry untuk error HTTP, implementasikan validasi kontrak, dan uji incremental loading secara eksplisit. Kafka Compose menyiapkan broker; producer dan consumer tetap harus dibuat/dilengkapi oleh mahasiswa.

# End-to-End Event-to-Warehouse Pipeline

## Deskripsi
Proyek ini membangun pipeline pengolahan transaksi e-commerce menggunakan Python, Pandas, Polars, Kafka, Pydantic, DuckDB, dan dbt.

## Alur Pipeline
1. Membaca data CSV melalui simulasi API dengan pagination.
2. Menyimpan data mentah ke Parquet dan menerapkan watermark.
3. Memvalidasi data menggunakan Pydantic.
4. Memisahkan data valid dan rejected.
5. Mengirim event transaksi melalui Kafka.
6. Mentransformasikan data dan membandingkan Pandas dengan Polars.
7. Menguji pengaruh data skew.
8. Memuat data valid ke DuckDB.
9. Membuat model dimensional dan tes menggunakan dbt.

## Cara Menjalankan
Jalankan skrip secara berurutan sesuai dokumentasi proyek:

- `scripts/check_data.py`
- `scripts/batch_ingest.py`
- `scripts/validate.py`
- `scripts/transform_data.py`
- `scripts/benchmark.py`
- `scripts/data_skew.py`
- `scripts/build_warehouse.py`
- `scripts/check_warehouse.py`

Kafka producer dan consumer dijalankan setelah broker aktif melalui Docker Compose. Model dbt dijalankan setelah file Parquet hasil transformasi tersedia.

## Catatan
CSV digunakan sebagai simulasi REST API lokal. Hasil benchmark harus diisi berdasarkan pengukuran aktual. Data rejected tidak boleh masuk ke warehouse. Implementasi Kafka saat ini perlu mekanisme idempotensi pada sink untuk mencegah duplikasi transaksi.
