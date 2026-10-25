-- One row per change of an order's status: creation, deletion, and updates that moved the
-- status. Snapshot reads (op r) and amount-only updates are not changes.
select
    event_id,
    order_id,
    op,
    before_status as from_status,
    status as to_status,
    source_ts as changed_at,
    source_lsn,
    kafka_offset
from {{ ref('stg_lake__orders_cdc_log') }}
where op in ('c', 'd')
    or (op = 'u' and before_status is distinct from status)
