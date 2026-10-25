"""Spark schemas of the current table and the merger's two control tables."""

from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from cdc_demo.ingest.schemas import AMOUNT

CURRENT_SCHEMA = StructType(
    [
        StructField("order_id", LongType(), nullable=False),
        StructField("customer_id", IntegerType()),
        StructField("status", StringType()),
        StructField("amount", AMOUNT),
        StructField("created_at", TimestampType()),
        StructField("updated_at", TimestampType()),
        StructField("_source_lsn", LongType()),
        StructField("_kafka_partition", IntegerType()),
        StructField("_kafka_offset", LongType()),
        StructField("_event_id", StringType()),
        StructField("_op", StringType()),
        StructField("_merged_at", TimestampType()),
        StructField("_merge_run_id", StringType()),
    ]
)

WATERMARK_SCHEMA = StructType(
    [
        StructField("kafka_topic", StringType()),
        StructField("kafka_partition", IntegerType()),
        StructField("last_offset", LongType()),
        StructField("updated_at", TimestampType()),
        StructField("run_id", StringType()),
    ]
)

# watermark_before / watermark_after are JSON strings: {"<topic>:<partition>": offset}.
MERGE_RUNS_SCHEMA = StructType(
    [
        StructField("run_id", StringType()),
        StructField("started_at", TimestampType()),
        StructField("finished_at", TimestampType()),
        StructField("cdc_log_version", LongType()),
        StructField("events_read", LongType()),
        StructField("distinct_orders", LongType()),
        StructField("rows_inserted", LongType()),
        StructField("rows_updated", LongType()),
        StructField("rows_deleted", LongType()),
        StructField("watermark_before", StringType()),
        StructField("watermark_after", StringType()),
    ]
)
