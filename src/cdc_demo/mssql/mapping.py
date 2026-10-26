"""Rows of the `mssql_cdc` source -> the CDC log contract. Pure DataFrame logic.

Input: the documented output schema of mssql-cdc-pyspark (six metadata columns, then the
captured columns of `dbo.orders`). Output: the same `ParsedBatch` the Kafka path produces, so
the merger, dbt and the verifiers read the result unchanged. See docs/PLAN.md for the table.

An update is two source rows, operation 3 (before) and 4 (after), which share
(`_start_lsn`, `_seqval`, `id`). The pair becomes one `u` event whose before columns come from
the operation 3 row. A 4 without a 3, or a 3 without a 4, is rejected as `unpaired_update`.
"""

from datetime import datetime

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from cdc_demo.ingest.parse import ParsedBatch
from cdc_demo.ingest.schemas import AMOUNT, CDC_LOG_SCHEMA, REJECTS_SCHEMA
from cdc_demo.mssql.lsn import BLOCK_LIMIT, VLF_LIMIT

TOPIC = "mssql.dbo.orders"
PARTITION = 0

REASON_NO_ORDER_ID = "null_order_id"
REASON_INVALID_OP = "invalid_op"
REASON_UNPAIRED = "unpaired_update"
REASON_LSN_RANGE = "lsn_out_of_range"

METADATA = ("_capture_instance", "_start_lsn", "_seqval", "_operation", "_command_id", "_commit_ts")
CAPTURED = ("id", "customer_id", "status", "amount", "created_at", "updated_at")
_VALID_OPS = (0, 1, 2, 4)
_PAIR_KEY = ("_start_lsn", "_seqval", "id")


def check_columns(columns: list[str]) -> None:
    """Fail loudly when the source schema is not the one this mapping was written for."""
    missing = [c for c in (*METADATA[1:], *CAPTURED) if c not in columns]
    extra = [c for c in columns if c not in METADATA and c not in CAPTURED]
    if missing or extra:
        raise RuntimeError(
            f"mssql_cdc output drifted from the expected schema: missing {missing}, extra {extra}"
        )


def _hex(start: int, length: int) -> Column:
    # Spark substring is 1-based; the text is `0x` + 20 hex digits.
    return F.conv(F.substring("_start_lsn", start, length), 16, 10).cast("long")


def map_changes(
    rows: DataFrame, batch_id: int, ingested_at: datetime, base_offset: int
) -> ParsedBatch:
    """`kafka_offset` is `base_offset + n` for the n-th emitted row (1-based) in library order
    `(_start_lsn, _command_id, _seqval, _operation)`; `id` only breaks the ties of a snapshot.

    The session time zone must be UTC (`_commit_ts` is a UTC wall time read as an instant).
    """
    check_columns(rows.columns)
    keep = [c for c in rows.columns if c != "_capture_instance"]
    df = rows.select(*keep, F.to_json(F.struct(*rows.columns)).alias("_raw"))
    op = F.col("_operation")

    # Operation 3 rows are consumed by their operation 4 row.
    before = df.where(op == 3).select(
        F.col("_start_lsn").alias("_b_lsn"),
        F.col("_seqval").alias("_b_seq"),
        F.col("id").alias("_b_id"),
        F.col("status").alias("_b_status"),
        F.col("amount").alias("_b_amount"),
        F.lit(True).alias("_has_before"),
    )
    before = before.dropDuplicates(["_b_lsn", "_b_seq", "_b_id"])
    on = (
        (F.col("_start_lsn") == F.col("_b_lsn"))
        & (F.col("_seqval") == F.col("_b_seq"))
        & (F.col("id") == F.col("_b_id"))
    )
    emitting = df.where(~F.coalesce(op == 3, F.lit(False))).join(before, on, "left")
    after_keys = df.where(op == 4).select(*_PAIR_KEY).dropDuplicates()
    orphans = (
        df.where(op == 3)
        .join(after_keys, list(_PAIR_KEY), "left_anti")
        .withColumn("_b_status", F.lit(None).cast("string"))
        .withColumn("_b_amount", F.lit(None).cast(AMOUNT))
        .withColumn("_has_before", F.lit(None).cast("boolean"))
    )
    combined = emitting.select(*df.columns, "_b_status", "_b_amount", "_has_before").unionByName(
        orphans.select(*df.columns, "_b_status", "_b_amount", "_has_before")
    )

    order = Window.orderBy("_start_lsn", "_command_id", "_seqval", "_operation", "id")
    numbered = combined.withColumn(
        "_offset", F.lit(base_offset).cast("long") + F.row_number().over(order)
    )

    fmt_ok = F.col("_start_lsn").rlike("^0x[0-9A-Fa-f]{20}$")
    vlf, block, slot = _hex(3, 8), _hex(11, 8), _hex(19, 4)
    in_range = fmt_ok & (vlf < VLF_LIMIT) & (block < BLOCK_LIMIT)
    packed = F.when(
        in_range, vlf * F.lit(1 << 40) + block * F.lit(1 << 16) + slot
    )  # CASE WHEN is lazy: no overflow for the rows that are rejected

    is_delete = op == 1
    c = F.col
    order_id = c("id")
    valid_op = F.coalesce(op.isin(*_VALID_OPS), F.lit(False))
    unpaired = (op == 3) | ((op == 4) & c("_has_before").isNull())
    reason = (
        F.when(~valid_op & ~F.coalesce(op == 3, F.lit(False)), F.lit(REASON_INVALID_OP))
        .when(~F.coalesce(in_range, F.lit(False)), F.lit(REASON_LSN_RANGE))
        .when(order_id.isNull(), F.lit(REASON_NO_ORDER_ID))
        .when(unpaired, F.lit(REASON_UNPAIRED))
    )
    op_letter = F.when(op == 0, "r").when(op == 2, "c").when(op == 4, "u").when(op == 1, "d")
    commit = c("_commit_ts").cast("timestamp")
    at = F.lit(ingested_at)
    batch = F.lit(batch_id).cast("bigint")
    classified = numbered.withColumn("_reason", reason)

    def after(name: str, dtype: str) -> Column:
        return F.when(~is_delete, c(name)).cast(dtype)

    # A delete row holds the deleted values: they are its before image.
    before_status = F.when(op == 4, c("_b_status")).when(is_delete, c("status"))
    before_amount = F.when(op == 4, c("_b_amount")).when(is_delete, c("amount"))
    valid = classified.where(c("_reason").isNull()).select(
        F.concat_ws("-", F.lit(TOPIC), F.lit(str(PARTITION)), c("_offset").cast("string")).alias(
            "event_id"
        ),
        op_letter.alias("op"),
        order_id.cast("long").alias("order_id"),
        after("customer_id", "int").alias("customer_id"),
        after("status", "string").alias("status"),
        after("amount", AMOUNT.simpleString()).alias("amount"),
        after("created_at", "timestamp").alias("created_at"),
        after("updated_at", "timestamp").alias("updated_at"),
        before_status.cast("string").alias("before_status"),
        before_amount.cast(AMOUNT.simpleString()).alias("before_amount"),
        F.unix_millis(commit).alias("source_ts_ms"),
        packed.alias("source_lsn"),
        F.lit(None).cast("long").alias("source_tx_id"),
        F.lit(TOPIC).alias("kafka_topic"),
        F.lit(PARTITION).alias("kafka_partition"),
        c("_offset").alias("kafka_offset"),
        commit.alias("kafka_timestamp"),
        batch.alias("ingest_batch_id"),
        at.alias("ingested_at"),
        F.to_date(F.coalesce(commit, at)).alias("_partition_date"),
    )
    rejects = classified.where(c("_reason").isNotNull()).select(
        c("_raw").alias("raw_value"),
        c("_reason").alias("reason"),
        F.lit(TOPIC).alias("kafka_topic"),
        F.lit(PARTITION).alias("kafka_partition"),
        c("_offset").alias("kafka_offset"),
        commit.alias("kafka_timestamp"),
        batch.alias("ingest_batch_id"),
        at.alias("ingested_at"),
        F.to_date(at).alias("_partition_date"),
    )
    if valid.columns != CDC_LOG_SCHEMA.names or rejects.columns != REJECTS_SCHEMA.names:
        raise RuntimeError("mssql mapping output columns drifted from the table schemas")
    return ParsedBatch(valid=valid, rejects=rejects, tombstones=0)
