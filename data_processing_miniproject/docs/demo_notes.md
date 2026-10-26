# Skenario Demo (5-8 menit)

Catatan persiapan: Docker Desktop berjalan, venv aktif, terminal di folder `data_processing_miniproject/`.
Untuk demo bersih gunakan group Kafka baru: `$env:KAFKA_GROUP_ID = "demo-1"`.
Ini naskah demo; rekaman video belum dibuat.

| Menit | Aspek rubrik | Perintah | Yang ditunjukkan |
|------:|--------------|----------|------------------|
| 0:00-0:30 | Pengantar | buka `pipeline_architecture.png` | dua jalur (batch dan streaming), sumber CSV mensimulasikan REST API |
| 0:30-1:45 | Batch ingestion | `python scripts/batch_ingest.py --simulate-failures 2` | halaman 10/10/10/6, retry 1 detik lalu 2 detik, sukses; jalankan ulang: "Record ditambahkan: 0", total unik 36 |
| 1:45-2:45 | Data contract | `python scripts/validate.py` | Valid 30, Rejected 6, rekonsiliasi OK, rekap penyebab; buka `data/rejected/orders_rejected.json` |
| 2:45-4:45 | Kafka + idempotensi | `docker compose ps`; `python scripts/producer.py`; `python scripts/send_duplicate_event.py`; `python scripts/consumer.py` | partition/offset, "DUPLIKAT (diabaikan)", sink berisi 1 baris `TEST-DUPLICATE-001`; jelaskan at-least-once dan batas jaminan |
| 4:45-5:45 | Transformasi dan performa | `python scripts/transform_data.py`; `python scripts/benchmark.py --sizes 30 10000 100000` | median Pandas vs Polars eager/lazy; sebutkan bahwa pada 30 baris selisih = noise |
| 5:45-7:00 | Warehouse dan dbt | `python scripts/build_warehouse.py`; `dbt run`; `dbt test`; `dbt docs serve` | PASS=5 dan PASS=14, lineage graph |
| 7:00-7:45 | Audit | `python scripts/audit_pipeline.py`; `python -m pytest -q` | 9/9 cek lulus; 35 tes lulus |
| 7:45-8:00 | Penutup | README bagian 14 | keterbatasan: CSV lokal, satu broker, bukan exactly-once end-to-end |

Pertanyaan yang mungkin diajukan dan lokasi jawabannya: README bagian 7 (konsep streaming), bagian 10-11 (benchmark dan skew), bagian 12 (star vs snowflake, incremental).
