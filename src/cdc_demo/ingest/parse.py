"""Raw Kafka batch -> (valid CDC rows, rejects, tombstone count). Pure DataFrame logic."""

from dataclasses import dataclass
from datetime import datetime

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.protobuf.functions import from_protobuf

from cdc_demo.ingest.schemas import AMOUNT, CDC_LOG_SCHEMA, REJECTS_SCHEMA
from cdc_demo.proto import VALUE_MESSAGE, value_descriptor_set

VALID_OPS = ("c", "u", "d", "r")

REASON_BAD_HEADER = "invalid_wire_header"
REASON_UNPARSEABLE = "unparseable_protobuf"
REASON_INVALID_OP = "invalid_op"
REASON_NO_AFTER = "missing_after_image"
REASON_NO_ORDER_ID = "null_order_id"
REASON_BAD_FIELD = "invalid_field"


@dataclass(frozen=True)
class ParsedBatch:
    valid: DataFrame
    rejects: DataFrame
    tombstones: int


def _ts(col: Column) -> Column:
    # try_*: Spark 4 runs in ANSI mode, where a bad value would fail the whole stream instead
    # of becoming a reject.
    return F.try_to_timestamp(col)


def parse_batch(raw: DataFrame, batch_id: int, ingested_at: datetime) -> ParsedBatch:
    """Split a raw Kafka batch (key, value, topic, partition, offset, timestamp).

    The value is a Confluent-framed Protobuf `Envelope`: magic byte 0, a 4-byte schema id and
    the message index `[0]` (one 0 byte), then the payload. The schema id is not used: the
    descriptor is compiled in. `raw_value` in rejects is the base64 of the value bytes.

    The session time zone must be UTC: `_partition_date` is derived from a timestamp.
    """
    base = raw.select(
        F.col("value"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    )
    tombstones = base.filter(F.col("value").isNull()).count()
    value = F.col("value")
    zero = F.lit(b"\0")
    header_ok = (
        (F.length(value) >= 6)
        & (F.substring(value, 1, 1) == zero)
        & (F.substring(value, 6, 1) == zero)
    )
    # Without a valid header the payload is not decoded at all. PERMISSIVE turns a payload that
    # fails to decode into a null struct instead of failing the stream.
    decoded = from_protobuf(
        F.expr("substring(value, 7)"),
        VALUE_MESSAGE,
        binaryDescriptorSet=value_descriptor_set(),
        options={"mode": "PERMISSIVE"},
    )
    env = F.when(header_ok, decoded)
    parsed = (
        base.filter(value.isNotNull())
        .withColumn("header_ok", header_ok)
        .withColumn("env", env)
        .withColumn("raw_value", F.base64(value))
    )

    e = F.col("env")
    after, before = e["after"], e["before"]
    order_id = F.coalesce(after["id"], before["id"])
    amount = after["amount"].try_cast(AMOUNT)
    before_amount = before["amount"].try_cast(AMOUNT)
    created_at = _ts(after["created_at"])
    updated_at = _ts(after["updated_at"])

    valid_op = F.coalesce(e["op"].isin(*VALID_OPS), F.lit(False))
    needs_after = e["op"].isin("c", "u", "r")
    bad_field = (
        (after["amount"].isNotNull() & amount.isNull())
        | (before["amount"].isNotNull() & before_amount.isNull())
        | (after["created_at"].isNotNull() & created_at.isNull())
        | (after["updated_at"].isNotNull() & updated_at.isNull())
    )
    # Proto3 scalars at their zero value decode as null (docs/DECISIONS.md, 26): an empty `op`
    # is an invalid op and a missing `after` image is null.
    reason = (
        F.when(~F.col("header_ok"), F.lit(REASON_BAD_HEADER))
        .when(e.isNull(), F.lit(REASON_UNPARSEABLE))
        .when(~valid_op, F.lit(REASON_INVALID_OP))
        .when(needs_after & after.isNull(), F.lit(REASON_NO_AFTER))
        .when(order_id.isNull(), F.lit(REASON_NO_ORDER_ID))
        .when(bad_field, F.lit(REASON_BAD_FIELD))
    )
    classified = parsed.withColumn("reason", reason)

    batch = F.lit(batch_id).cast("bigint")
    at = F.lit(ingested_at)
    source_ts = e["source"]["ts_ms"]
    event_id = F.concat_ws(
        "-",
        F.col("kafka_topic"),
        F.col("kafka_partition").cast("string"),
        F.col("kafka_offset").cast("string"),
    )
    # Fall back to the Kafka timestamp so a row never lands in the null partition.
    partition_date = F.to_date(F.coalesce(F.timestamp_millis(source_ts), F.col("kafka_timestamp")))

    valid = classified.filter(F.col("reason").isNull()).select(
        event_id.alias("event_id"),
        e["op"].alias("op"),
        order_id.alias("order_id"),
        after["customer_id"].alias("customer_id"),
        after["status"].alias("status"),
        amount.alias("amount"),
        created_at.alias("created_at"),
        updated_at.alias("updated_at"),
        before["status"].alias("before_status"),
        before_amount.alias("before_amount"),
        source_ts.alias("source_ts_ms"),
        e["source"]["lsn"].alias("source_lsn"),
        e["source"]["txId"].alias("source_tx_id"),
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        batch.alias("ingest_batch_id"),
        at.alias("ingested_at"),
        partition_date.alias("_partition_date"),
    )
    rejects = classified.filter(F.col("reason").isNotNull()).select(
        "raw_value",
        "reason",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        batch.alias("ingest_batch_id"),
        at.alias("ingested_at"),
        F.to_date(at).alias("_partition_date"),
    )
    if valid.columns != CDC_LOG_SCHEMA.names or rejects.columns != REJECTS_SCHEMA.names:
        raise RuntimeError("parse output columns drifted from the table schemas")
    return ParsedBatch(valid=valid, rejects=rejects, tombstones=tombstones)
