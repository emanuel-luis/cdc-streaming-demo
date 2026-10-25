select
    date_trunc('hour', source_ts) as activity_hour,
    op,
    count(*) as events,
    count(distinct order_id) as distinct_orders
from {{ ref('stg_lake__orders_cdc_log') }}
group by 1, 2
