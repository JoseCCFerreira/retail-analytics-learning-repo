select
    cast(transaction_id as bigint) as transaction_id,
    cast(transaction_date as date) as transaction_date,
    cast(customer_id as bigint) as customer_id,
    cast(store_id as bigint) as store_id,
    lower(payment_method) as payment_method
from transactions
