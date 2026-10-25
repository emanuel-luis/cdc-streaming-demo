select order_id, amount
from {{ ref('dim_orders_current') }}
where amount <= 0
