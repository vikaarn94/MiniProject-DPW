
import duckdb

DB_PATH = "data/warehouse/ecommerce.duckdb"

con = duckdb.connect(DB_PATH, read_only=True)

try:
    print("Jumlah transaksi per status:")
    print(con.execute("""
        SELECT status, COUNT(*) AS total
        FROM fct_orders
        GROUP BY status
        ORDER BY total DESC
    """).df())

    print("\nPenjualan harian:")
    print(con.execute("""
        SELECT *
        FROM mart_daily_sales
        ORDER BY order_date
        LIMIT 10
    """).df())
finally:
    con.close()
