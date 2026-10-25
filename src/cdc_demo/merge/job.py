"""One batch merge run: pinned CDC log read -> dedup -> guarded MERGE -> watermark -> run row."""

import json
import logging
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from cdc_demo.logging import log_event
from cdc_demo.merge.schemas import CURRENT_SCHEMA, MERGE_RUNS_SCHEMA, WATERMARK_SCHEMA
from cdc_demo.settings import LakeSettings
from cdc_demo.spark.tables import create_table

log = logging.getLogger(__name__)

# Decision 13: update only when the incoming event is newer than the stored one.
NEWER = (
    "s._source_lsn > t._source_lsn "
    "OR (s._source_lsn = t._source_lsn AND s._kafka_offset > t._kafka_offset)"
)
LIVE_OPS = "s._op IN ('c', 'u', 'r')"
_COUNTERS = ("events_read", "distinct_orders", "rows_inserted", "rows_updated", "rows_deleted")

Offsets = dict[tuple[str, int], int]


@dataclass(frozen=True)
class MergeResult:
    run_id: str
    started_at: datetime
    finished_at: datetime
    cdc_log_version: int | None
    events_read: int
    distinct_orders: int
    rows_inserted: int
    rows_updated: int
    rows_deleted: int
    watermark_before: str
    watermark_after: str

    def summary(self) -> str:
        return (
            f"run {self.run_id}: events read {self.events_read}, orders {self.distinct_orders}, "
            f"inserted {self.rows_inserted}, updated {self.rows_updated}, "
            f"deleted {self.rows_deleted}\n"
            f"watermark before: {self.watermark_before}\n"
            f"watermark after:  {self.watermark_after}"
        )


def watermark_json(offsets: Offsets) -> str:
    return json.dumps({f"{t}:{p}": o for (t, p), o in sorted(offsets.items())})


def create_tables(spark: SparkSession, lake: LakeSettings) -> None:
    """Decision 30: explicit schemas, so an empty target merges like any other."""
    for path, schema in (
        (lake.current_orders, CURRENT_SCHEMA),
        (lake.merge_watermark, WATERMARK_SCHEMA),
        (lake.merge_runs, MERGE_RUNS_SCHEMA),
    ):
        create_table(spark, path, schema)


def _read_watermark(spark: SparkSession, lake: LakeSettings) -> Offsets:
    rows = spark.read.format("delta").load(lake.merge_watermark).collect()
    return {(r["kafka_topic"], r["kafka_partition"]): r["last_offset"] for r in rows}


def _latest_version(spark: SparkSession, path: str) -> int:
    row = DeltaTable.forPath(spark, path).history(1).select("version").first()
    if row is None:
        raise RuntimeError(f"no history for {path}")
    return int(row[0])


def _incremental(spark: SparkSession, lake: LakeSettings, version: int) -> DataFrame:
    """Events above the watermark, from one read of the CDC log at a pinned version."""
    pinned = spark.read.format("delta").option("versionAsOf", version).load(lake.cdc_log)
    wm = (
        spark.read.format("delta")
        .load(lake.merge_watermark)
        .select(
            F.col("kafka_topic").alias("wm_topic"),
            F.col("kafka_partition").alias("wm_partition"),
            F.col("last_offset").alias("wm_offset"),
        )
    )
    on = (F.col("kafka_topic") == F.col("wm_topic")) & (
        F.col("kafka_partition") == F.col("wm_partition")
    )
    return (
        pinned.join(F.broadcast(wm), on, "left")
        .where(F.col("wm_offset").isNull() | (F.col("kafka_offset") > F.col("wm_offset")))
        .drop("wm_topic", "wm_partition", "wm_offset")
    )


def dedup(events: DataFrame) -> DataFrame:
    """One event per order_id, the latest by (source_lsn, kafka_offset). Decision 12."""
    w = Window.partitionBy("order_id").orderBy(
        F.col("source_lsn").desc(), F.col("kafka_offset").desc()
    )
    return events.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def _source(latest: DataFrame, run_id: str, merged_at: datetime) -> DataFrame:
    return latest.select(
        "order_id",
        "customer_id",
        "status",
        "amount",
        "created_at",
        "updated_at",
        F.col("source_lsn").alias("_source_lsn"),
        F.col("kafka_partition").alias("_kafka_partition"),
        F.col("kafka_offset").alias("_kafka_offset"),
        F.col("event_id").alias("_event_id"),
        F.col("op").alias("_op"),
        F.lit(merged_at).cast("timestamp").alias("_merged_at"),
        F.lit(run_id).alias("_merge_run_id"),
    )


def _merge(spark: SparkSession, lake: LakeSettings, source: DataFrame) -> dict[str, int]:
    """Run the guarded MERGE and return its operation metrics (zeros if it committed nothing)."""
    target = DeltaTable.forPath(spark, lake.current_orders)
    before = _latest_version(spark, lake.current_orders)
    (
        target.alias("t")
        .merge(source.alias("s"), "t.order_id = s.order_id")
        .whenMatchedDelete(condition=f"s._op = 'd' AND ({NEWER})")
        .whenMatchedUpdateAll(condition=f"{LIVE_OPS} AND ({NEWER})")
        .whenNotMatchedInsertAll(condition=LIVE_OPS)
        .execute()
    )
    last = target.history(1).select("version", "operation", "operationMetrics").first()
    metrics: dict[str, str] = {}
    if last is not None and last["version"] > before and last["operation"] == "MERGE":
        metrics = last["operationMetrics"] or {}
    return {
        "rows_inserted": int(metrics.get("numTargetRowsInserted", 0)),
        "rows_updated": int(metrics.get("numTargetRowsUpdated", 0)),
        "rows_deleted": int(metrics.get("numTargetRowsDeleted", 0)),
    }


def _advance_watermark(
    spark: SparkSession, lake: LakeSettings, new: Offsets, run_id: str, now: datetime
) -> None:
    """Move the watermark. Called only after the MERGE has committed (decision 13)."""
    rows = [(t, p, o, now, run_id) for (t, p), o in new.items()]
    src = spark.createDataFrame(rows, WATERMARK_SCHEMA)
    (
        DeltaTable.forPath(spark, lake.merge_watermark)
        .alias("t")
        .merge(
            src.alias("s"),
            "t.kafka_topic = s.kafka_topic AND t.kafka_partition = s.kafka_partition",
        )
        .whenMatchedUpdateAll(condition="s.last_offset > t.last_offset")
        .whenNotMatchedInsertAll()
        .execute()
    )


def _finish(
    spark: SparkSession,
    lake: LakeSettings,
    *,
    run_id: str,
    started_at: datetime,
    version: int | None,
    counts: dict[str, int],
    before: Offsets,
    after: Offsets,
) -> MergeResult:
    result = MergeResult(
        run_id=run_id,
        started_at=started_at,
        finished_at=datetime.now(UTC),
        cdc_log_version=version,
        watermark_before=watermark_json(before),
        watermark_after=watermark_json(after),
        **{k: counts.get(k, 0) for k in _COUNTERS},
    )
    row: dict[str, Any] = asdict(result)
    values = tuple(row[f.name] for f in MERGE_RUNS_SCHEMA.fields)
    df = spark.createDataFrame([values], MERGE_RUNS_SCHEMA)
    df.write.format("delta").mode("append").save(lake.merge_runs)
    log_event(
        log,
        "merge_finished",
        run_id=run_id,
        cdc_log_version=version,
        watermark_before=result.watermark_before,
        watermark_after=result.watermark_after,
        **{k: getattr(result, k) for k in _COUNTERS},
    )
    return result


def run_merge(spark: SparkSession, lake: LakeSettings, run_id: str | None = None) -> MergeResult:
    run_id = run_id or uuid.uuid4().hex
    started_at = datetime.now(UTC)
    create_tables(spark, lake)
    before = _read_watermark(spark, lake)
    if not DeltaTable.isDeltaTable(spark, lake.cdc_log):
        return _finish(
            spark,
            lake,
            run_id=run_id,
            started_at=started_at,
            version=None,
            counts={},
            before=before,
            after=before,
        )

    version = _latest_version(spark, lake.cdc_log)
    events = _incremental(spark, lake, version).persist()
    try:
        stats = events.agg(
            F.count(F.lit(1)).alias("n"), F.countDistinct("order_id").alias("orders")
        ).first()
        if stats is None:
            raise RuntimeError("aggregate over the events returned no row")
        counts: dict[str, int] = {"events_read": stats["n"], "distinct_orders": stats["orders"]}
        after = dict(before)
        if stats["n"]:
            source = _source(dedup(events), run_id, datetime.now(UTC))
            counts.update(_merge(spark, lake, source))
            per_partition = (
                events.groupBy("kafka_topic", "kafka_partition")
                .agg(F.max("kafka_offset").alias("hi"))
                .collect()
            )
            new = {(r["kafka_topic"], r["kafka_partition"]): r["hi"] for r in per_partition}
            _advance_watermark(spark, lake, new, run_id, datetime.now(UTC))
            after.update(new)
    finally:
        events.unpersist()
    return _finish(
        spark,
        lake,
        run_id=run_id,
        started_at=started_at,
        version=version,
        counts=counts,
        before=before,
        after=after,
    )
