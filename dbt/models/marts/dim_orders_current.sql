select
    order_id,
    customer_id,
    status,
    amount,
    created_at,
    updated_at,
    merged_at
from {{ ref('stg_lake__orders_current') }}
