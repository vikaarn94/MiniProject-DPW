import json
import os
from pathlib import Path

from kafka import KafkaConsumer
from stream_sink import DUPLICATE, INSERTED, REJECTED, open_sink, process_event

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "orders")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "orders-validation-group")

VALID_PATH = Path("data/stream/valid.jsonl")
REJECTED_PATH = Path("data/stream/rejected.jsonl")
DUPLICATE_PATH = Path("data/stream/duplicates.jsonl")


def write_jsonl(path, record):
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("a", encoding="utf-8") as file:
        file.write(
            json.dumps(
                record,
                ensure_ascii=False,
                default=str
            ) + "\n"
        )


def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        ),
        consumer_timeout_ms=15000,
    )

    sink = open_sink()
    counts = {INSERTED: 0, DUPLICATE: 0, REJECTED: 0}

    try:
        for message in consumer:
            payload = message.value

            metadata = {
                "topic": message.topic,
                "partition": message.partition,
                "offset": message.offset,
            }

            # 1) Tulis ke sink idempoten (atomik, kunci order_id).
            outcome, order, reason = process_event(
                sink,
                payload,
                partition=message.partition,
                offset=message.offset,
            )
            counts[outcome] += 1

            # 2) File jsonl hanya jejak audit; sink DuckDB adalah sumber kebenaran.
            if outcome == INSERTED:
                write_jsonl(
                    VALID_PATH,
                    {**metadata, "order": order.model_dump(mode="json")},
                )
                print(
                    f"VALID: {order.order_id} | "
                    f"partition={message.partition} | "
                    f"offset={message.offset}"
                )

            elif outcome == DUPLICATE:
                write_jsonl(
                    DUPLICATE_PATH,
                    {**metadata, "order_id": order.order_id},
                )
                print(
                    f"DUPLIKAT (diabaikan): {order.order_id} | "
                    f"partition={message.partition} | "
                    f"offset={message.offset}"
                )

            else:
                write_jsonl(
                    REJECTED_PATH,
                    {**metadata, "payload": payload, "error": reason},
                )
                print(f"REJECTED: {payload.get('order_id')} | {reason}")

            # 3) Commit offset terakhir. Bila proses mati sebelum commit,
            #    Kafka mengirim ulang pesan (at-least-once); sink menolak
            #    duplikatnya lewat PRIMARY KEY sehingga tidak ada efek ganda.
            consumer.commit()

        total = sink.execute(
            "SELECT COUNT(*) FROM stream_orders"
        ).fetchone()[0]

        print("\n=== RINGKASAN CONSUMER ===")
        print("Pesan dibaca  :", sum(counts.values()))
        print("Valid (baru)  :", counts[INSERTED])
        print("Duplikat      :", counts[DUPLICATE])
        print("Rejected      :", counts[REJECTED])
        print("Baris di sink :", total)
        print("File valid    :", VALID_PATH)
        print("File duplikat :", DUPLICATE_PATH)
        print("File rejected :", REJECTED_PATH)

    finally:
        consumer.close()
        sink.close()


if __name__ == "__main__":
    main()
