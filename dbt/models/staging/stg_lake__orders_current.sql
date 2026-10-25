select
    order_id,
    customer_id,
    status,
    amount,
    created_at,
    updated_at,
    _source_lsn as source_lsn,
    _kafka_partition as kafka_partition,
    _kafka_offset as kafka_offset,
    _event_id as event_id,
    _op as op,
    _merged_at as merged_at,
    _merge_run_id as merge_run_id
from {{ source('lake', 'orders_current') }}
