
WITH ranked_orders AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY order_ts
        ) AS row_num
    FROM {{ ref('stg_orders') }}
),

deduplicated AS (
    SELECT *
    FROM ranked_orders
    WHERE row_num = 1
)

SELECT
    o.order_id,
    o.customer_id,
    o.product_category,
    o.order_ts,
    CAST(o.order_ts AS DATE) AS order_date,
    o.quantity,
    o.unit_price,
    o.discount_pct,
    o.gross_amount,
    o.net_amount,
    o.payment_method,
    o.status
FROM deduplicated o
INNER JOIN {{ ref('dim_customer') }} c
    ON o.customer_id = c.customer_id
INNER JOIN {{ ref('dim_product_category') }} p
    ON o.product_category = p.product_category
