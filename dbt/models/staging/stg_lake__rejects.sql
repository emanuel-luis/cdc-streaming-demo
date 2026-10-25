-- raw_value is left out on purpose: a rejected payload is not analytics data.
select
    reason,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    kafka_timestamp,
    ingest_batch_id,
    ingested_at
from {{ source('lake', 'rejects') }}
