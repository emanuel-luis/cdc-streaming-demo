select
    run_id,
    batch_id,
    started_at,
    finished_at,
    input_rows,
    tombstones,
    valid_rows,
    rejected_rows,
    min_offset,
    max_offset,
    kafka_topic
from {{ source('lake', 'ingest_runs') }}
