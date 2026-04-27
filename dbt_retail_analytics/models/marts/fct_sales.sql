select
    si.sales_item_id,
    t.transaction_id,
    t.transaction_date,
    t.customer_id,
    t.store_id,
    t.payment_method,
    si.product_id,
    si.quantity,
    si.unit_price,
    si.discount_pct,
    si.gross_amount,
    si.discount_amount,
    si.net_amount
from {{ ref('stg_sales_items') }} si
inner join {{ ref('stg_transactions') }} t
    on t.transaction_id = si.transaction_id
