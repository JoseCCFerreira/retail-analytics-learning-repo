select
    cast(product_id as bigint) as product_id,
    product_name,
    category,
    cast(unit_price as double) as unit_price
from products
