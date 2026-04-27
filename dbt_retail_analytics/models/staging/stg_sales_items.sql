select
    cast(sales_item_id as bigint) as sales_item_id,
    cast(transaction_id as bigint) as transaction_id,
    cast(product_id as bigint) as product_id,
    cast(quantity as integer) as quantity,
    cast(unit_price as double) as unit_price,
    cast(discount_pct as integer) as discount_pct,
    cast(gross_amount as double) as gross_amount,
    cast(discount_amount as double) as discount_amount,
    cast(net_amount as double) as net_amount
from sales_items
