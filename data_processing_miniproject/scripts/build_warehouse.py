
from pathlib import Path
import duckdb

INPUT_PATH = "data/warehouse/orders_transformed.parquet"
DB_PATH = "data/warehouse/ecommerce.duckdb"


def main():
    if not Path(INPUT_PATH).exists():
        raise FileNotFoundError(
            f"File tidak ditemukan: {INPUT_PATH}. "
            "Jalankan transform_data.py terlebih dahulu."
        )

    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(DB_PATH)

    try:
        # Staging: data valid hasil transformasi
        con.execute("""
            CREATE OR REPLACE TABLE stg_orders AS
            SELECT
                CAST(order_id AS VARCHAR) AS order_id,
                CAST(customer_id AS VARCHAR) AS customer_id,
                CAST(order_ts AS TIMESTAMP WITH TIME ZONE) AS order_ts,
                CAST(product_category AS VARCHAR) AS product_category,
                CAST(quantity AS INTEGER) AS quantity,
                CAST(unit_price AS DOUBLE) AS unit_price,
                CAST(discount_pct AS DOUBLE) AS discount_pct,
                CAST(payment_method AS VARCHAR) AS payment_method,
                CAST(status AS VARCHAR) AS status,
                CAST(gross_amount AS DOUBLE) AS gross_amount,
                CAST(net_amount AS DOUBLE) AS net_amount
            FROM read_parquet(?)
        """, [INPUT_PATH])

        # Dimensi customer
        con.execute("""
            CREATE OR REPLACE TABLE dim_customer AS
            SELECT DISTINCT customer_id
            FROM stg_orders
        """)

        # Dimensi kategori produk
        con.execute("""
            CREATE OR REPLACE TABLE dim_product_category AS
            SELECT DISTINCT product_category
            FROM stg_orders
        """)

        # Fact table: satu baris per order_id
        # Jika order_id duplikat, pilih satu baris agar tidak
        # menggandakan transaksi di fact table.
        con.execute("""
            CREATE OR REPLACE TABLE fct_orders AS
            SELECT
                s.order_id,
                s.customer_id,
                s.product_category,
                s.order_ts,
                CAST(s.order_ts AS DATE) AS order_date,
                s.quantity,
                s.unit_price,
                s.discount_pct,
                s.gross_amount,
                s.net_amount,
                s.payment_method,
                s.status
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY order_id
                        ORDER BY order_ts
                    ) AS rn
                FROM stg_orders
            ) AS s
            INNER JOIN dim_customer c
                ON s.customer_id = c.customer_id
            INNER JOIN dim_product_category p
                ON s.product_category = p.product_category
            WHERE s.rn = 1
        """)

        # Mart penjualan harian.
        # Definisi penjualan di sini hanya paid dan shipped.
        con.execute("""
            CREATE OR REPLACE TABLE mart_daily_sales AS
            SELECT
                order_date,
                product_category,
                payment_method,
                COUNT(*) AS total_orders,
                SUM(quantity) AS total_quantity,
                SUM(net_amount) AS net_sales
            FROM fct_orders
            WHERE status IN ('paid', 'shipped')
            GROUP BY
                order_date, product_category, payment_method
        """)

        for table in [
            "stg_orders",
            "dim_customer",
            "dim_product_category",
            "fct_orders",
            "mart_daily_sales",
        ]:
            count = con.execute(
                f"SELECT COUNT(*) FROM {table}"
            ).fetchone()[0]
            print(f"{table}: {count} baris")

        print(f"\nWarehouse tersimpan di: {DB_PATH}")

        print("\nContoh hasil mart_daily_sales:")
        print(con.execute("""
            SELECT *
            FROM mart_daily_sales
            ORDER BY order_date, product_category
            LIMIT 10
        """).df())

    finally:
        con.close()


if __name__ == "__main__":
    main()
