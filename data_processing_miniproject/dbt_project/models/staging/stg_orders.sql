
SELECT
    CAST(order_id AS VARCHAR) AS order_id,
    CAST(customer_id AS VARCHAR) AS customer_id,
    CAST(order_ts AS TIMESTAMP) AS order_ts,
    CAST(product_category AS VARCHAR) AS product_category,
    CAST(quantity AS INTEGER) AS quantity,
    CAST(unit_price AS DOUBLE) AS unit_price,
    CAST(discount_pct AS DOUBLE) AS discount_pct,
    CAST(payment_method AS VARCHAR) AS payment_method,
    CAST(status AS VARCHAR) AS status,
    CAST(quantity * unit_price AS DOUBLE) AS gross_amount,
    CAST(
        quantity * unit_price * (1 - discount_pct)
        AS DOUBLE
    ) AS net_amount
FROM read_parquet('data/warehouse/orders_transformed.parquet')
