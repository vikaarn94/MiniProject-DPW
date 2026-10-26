-- Gagal bila ada baris yang gross/net_amount-nya tidak sesuai rumus kontrak:
--   gross_amount = quantity * unit_price
--   net_amount   = gross_amount * (1 - discount_pct)
SELECT order_id
FROM {{ ref('fct_orders') }}
WHERE ABS(gross_amount - quantity * unit_price) > 1e-9
   OR ABS(net_amount - gross_amount * (1 - discount_pct)) > 1e-9
