-- One row per consumer batch and per merge run.
select
    'ingest' as component,
    run_id,
    batch_id,
    started_at,
    finished_at,
    date_diff('millisecond', started_at, finished_at) / 1000.0 as duration_seconds,
    input_rows,
    valid_rows as output_rows,
    rejected_rows
from {{ ref('stg_lake__ingest_runs') }}

union all

select
    'merge' as component,
    run_id,
    null::bigint as batch_id,
    started_at,
    finished_at,
    date_diff('millisecond', started_at, finished_at) / 1000.0 as duration_seconds,
    events_read as input_rows,
    rows_inserted + rows_updated + rows_deleted as output_rows,
    0::bigint as rejected_rows
from {{ ref('stg_lake__merge_runs') }}
