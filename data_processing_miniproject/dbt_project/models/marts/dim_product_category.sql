
SELECT DISTINCT
    product_category
FROM {{ ref('stg_orders') }}
WHERE product_category IS NOT NULL
