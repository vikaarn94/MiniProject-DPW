
SELECT DISTINCT
    customer_id
FROM {{ ref('stg_orders') }}
WHERE customer_id IS NOT NULL
