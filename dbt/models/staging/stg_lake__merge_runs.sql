select
    run_id,
    started_at,
    finished_at,
    cdc_log_version,
    events_read,
    distinct_orders,
    rows_inserted,
    rows_updated,
    rows_deleted,
    watermark_before,
    watermark_after
from {{ source('lake', 'merge_runs') }}
