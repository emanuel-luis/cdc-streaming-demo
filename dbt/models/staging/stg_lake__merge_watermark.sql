select
    kafka_topic,
    kafka_partition,
    last_offset,
    updated_at,
    run_id
from {{ source('lake', 'merge_watermark') }}
