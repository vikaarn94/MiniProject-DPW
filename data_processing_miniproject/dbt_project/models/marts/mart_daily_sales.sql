
SELECT
    order_date,
    product_category,
    payment_method,
    COUNT(*) AS total_orders,
    SUM(quantity) AS total_quantity,
    SUM(net_amount) AS net_sales
FROM {{ ref('fct_orders') }}
WHERE status IN ('paid', 'shipped')
GROUP BY
    order_date,
    product_category,
    payment_method
