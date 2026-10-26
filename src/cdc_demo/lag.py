"""End-to-end lag of the CDC log: batch finish time minus the source commit time.

Works on either lake (LAKE_ROOT). Only c, u and d events count: snapshot rows carry the
snapshot's own commit time, which says nothing about streaming lag.
"""

import math
from dataclasses import dataclass
from typing import Any

from cdc_demo.settings import LakeSettings, S3Settings, SparkSettings


@dataclass(frozen=True)
class LagStats:
    events: int
    p50_s: float
    p95_s: float
    max_s: float

    def report(self) -> str:
        return (
            f"events  {self.events}\n"
            f"p50     {self.p50_s:.2f} s\n"
            f"p95     {self.p95_s:.2f} s\n"
            f"max     {self.max_s:.2f} s"
        )


def percentile(sorted_values: list[float], q: float) -> float:
    """Nearest-rank percentile of a sorted, non-empty list; q in (0, 100]."""
    rank = max(1, math.ceil(q / 100 * len(sorted_values)))
    return sorted_values[rank - 1]


def lag_stats(lags_ms: list[int]) -> LagStats | None:
    if not lags_ms:
        return None
    seconds = sorted(v / 1000 for v in lags_ms)
    return LagStats(len(seconds), percentile(seconds, 50), percentile(seconds, 95), seconds[-1])


def collect_lags_ms(spark: Any, lake: LakeSettings) -> list[int]:
    # ponytail: collects one long per event to the driver; fine at demo scale.
    from pyspark.sql import functions as F

    events = (
        spark.read.format("delta")
        .load(lake.cdc_log)
        .where(F.col("op").isin("c", "u", "d") & F.col("source_ts_ms").isNotNull())
        .select("ingest_batch_id", "source_ts_ms")
    )
    runs = (
        spark.read.format("delta")
        .load(lake.ingest_runs)
        .select(F.col("batch_id").alias("ingest_batch_id"), "finished_at")
    )
    lag = events.join(runs, "ingest_batch_id").select(
        (F.unix_millis("finished_at") - F.col("source_ts_ms")).alias("lag_ms")
    )
    return [int(r["lag_ms"]) for r in lag.collect()]


def run_lag_report(lake: LakeSettings, s3: S3Settings | None, spark_settings: SparkSettings) -> int:
    from cdc_demo.spark.session import build_session

    spark = build_session(spark_settings, s3, kafka=False, app_name="cdc-demo-lag-report")
    try:
        stats = lag_stats(collect_lags_ms(spark, lake))
    finally:
        spark.stop()
    if stats is None:
        print("no c, u or d events in the CDC log yet")
        return 1
    print(stats.report())
    return 0
