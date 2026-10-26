import json
from collections import Counter
from pathlib import Path

import pandas as pd
from contract import CONTRACT_ERRORS, reason_code, validate_record


def validate_dataframe(df):
    """Pisahkan DataFrame menjadi record valid dan rejected (beserta alasan)."""
    valid_orders = []
    rejected_orders = []

    for index, row in df.iterrows():
        original = row.to_dict()

        # Normalisasi timestamp menjadi string ISO.
        if pd.notna(original.get("order_ts")):
            original["order_ts"] = (
                pd.Timestamp(original["order_ts"]).isoformat()
            )
        else:
            original["order_ts"] = None

        try:
            order = validate_record(original)

            valid_orders.append(
                order.model_dump(mode="json")
            )

        except CONTRACT_ERRORS as exc:
            rejected_orders.append({
                "row_number": int(index) + 2,
                "order_id": original.get("order_id"),
                "reason_code": reason_code(exc),
                "payload": original,
                "error": str(exc),
            })

    return valid_orders, rejected_orders


def build_audit(total, valid_orders, rejected_orders):
    """Rekonsiliasi jumlah: total input harus = valid + rejected."""
    reasons = Counter(item["reason_code"] for item in rejected_orders)

    return {
        "total_input": total,
        "valid": len(valid_orders),
        "rejected": len(rejected_orders),
        "reconciled": total == len(valid_orders) + len(rejected_orders),
        "rejected_by_reason": dict(reasons.most_common()),
    }


def main():
    source_path = Path("data/raw/orders.parquet")
    valid_dir = Path("data/valid")
    rejected_dir = Path("data/rejected")

    valid_dir.mkdir(parents=True, exist_ok=True)
    rejected_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(source_path)

    valid_orders, rejected_orders = validate_dataframe(df)
    audit = build_audit(len(df), valid_orders, rejected_orders)

    valid_path = valid_dir / "orders_valid.parquet"
    rejected_path = rejected_dir / "orders_rejected.json"
    audit_path = rejected_dir / "validation_audit.json"

    pd.DataFrame(valid_orders).to_parquet(
        valid_path,
        index=False
    )

    rejected_path.write_text(
        json.dumps(
            rejected_orders,
            indent=2,
            ensure_ascii=False,
            default=str
        ),
        encoding="utf-8"
    )

    audit_path.write_text(
        json.dumps(audit, indent=2, ensure_ascii=False),
        encoding="utf-8"
    )

    print("\n=== HASIL VALIDASI ===")
    print("Total record :", audit["total_input"])
    print("Valid        :", audit["valid"])
    print("Rejected     :", audit["rejected"])
    print("Rekonsiliasi :", "OK (total = valid + rejected)"
          if audit["reconciled"] else "TIDAK SESUAI")
    print("File valid   :", valid_path)
    print("File rejected:", rejected_path)
    print("File audit   :", audit_path)

    print("\nPenyebab penolakan (terbanyak dulu):")

    for code, count in audit["rejected_by_reason"].items():
        print(f"  {code}: {count}")

    print("\nContoh alasan penolakan:")

    for item in rejected_orders[:5]:
        print(
            item["order_id"],
            "->",
            item["error"]
        )

    if not audit["reconciled"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
