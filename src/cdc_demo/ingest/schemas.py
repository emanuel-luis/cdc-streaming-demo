"""Spark schemas of the three tables the consumer writes."""

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

# Decimals arrive as strings (decimal.handling.mode=string), so the Envelope in
# cdc_demo/proto carries `amount` as a string; parse.py casts it to this type.

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
