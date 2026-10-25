"""Spark schemas: the Debezium JSON envelope and the three tables the consumer writes."""

from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Debezium with schemas.enable=false: decimals arrive as strings
# (decimal.handling.mode=string) and TIMESTAMPTZ as ISO-8601 strings (ZonedTimestamp).
ROW_SCHEMA = StructType(
    [
        StructField("id", LongType()),
        StructField("customer_id", IntegerType()),
        StructField("status", StringType()),
        StructField("amount", StringType()),
        StructField("created_at", StringType()),
        StructField("updated_at", StringType()),
    ]
)

SOURCE_SCHEMA = StructType(
    [
        StructField("ts_ms", LongType()),
        StructField("lsn", LongType()),
        StructField("txId", LongType()),
        StructField("snapshot", StringType()),
        StructField("db", StringType()),
        StructField("schema", StringType()),
        StructField("table", StringType()),
    ]
)

ENVELOPE_SCHEMA = StructType(
    [
        StructField("before", ROW_SCHEMA),
        StructField("after", ROW_SCHEMA),
        StructField("source", SOURCE_SCHEMA),
        StructField("op", StringType()),
        StructField("ts_ms", LongType()),
    ]
)

AMOUNT = DecimalType(12, 2)

CDC_LOG_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("op", StringType()),
        StructField("order_id", LongType()),
        StructField("customer_id", IntegerType()),
        StructField("status", StringType()),
        StructField("amount", AMOUNT),
        StructField("created_at", TimestampType()),
        StructField("updated_at", TimestampType()),
        StructField("before_status", StringType()),
        StructField("before_amount", AMOUNT),
        StructField("source_ts_ms", LongType()),
        StructField("source_lsn", LongType()),
        StructField("source_tx_id", LongType()),
        StructField("kafka_topic", StringType()),
        StructField("kafka_partition", IntegerType()),
        StructField("kafka_offset", LongType()),
        StructField("kafka_timestamp", TimestampType()),
        StructField("ingest_batch_id", LongType()),
        StructField("ingested_at", TimestampType()),
        StructField("_partition_date", DateType()),
    ]
)

REJECTS_SCHEMA = StructType(
    [
        StructField("raw_value", StringType()),
        StructField("reason", StringType()),
        StructField("kafka_topic", StringType()),
        StructField("kafka_partition", IntegerType()),
        StructField("kafka_offset", LongType()),
        StructField("kafka_timestamp", TimestampType()),
        StructField("ingest_batch_id", LongType()),
        StructField("ingested_at", TimestampType()),
        # From ingest time: a reject may have no usable source timestamp.
        StructField("_partition_date", DateType()),
    ]
)

INGEST_RUNS_SCHEMA = StructType(
    [
        StructField("run_id", StringType()),
        StructField("batch_id", LongType()),
        StructField("started_at", TimestampType()),
        StructField("finished_at", TimestampType()),
        StructField("input_rows", LongType()),
        StructField("tombstones", LongType()),
        StructField("valid_rows", LongType()),
        StructField("rejected_rows", LongType()),
        StructField("min_offset", LongType()),
        StructField("max_offset", LongType()),
        StructField("kafka_topic", StringType()),
    ]
)
