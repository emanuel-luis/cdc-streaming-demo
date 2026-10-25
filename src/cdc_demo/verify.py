"""End-to-end assertions: the lake and Postgres must match what the mutator reported."""

import logging
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import psycopg

from cdc_demo.mutator.db import count_orders
from cdc_demo.mutator.model import Manifest
from cdc_demo.settings import LakeSettings, PostgresSettings, S3Settings, SparkSettings

log = logging.getLogger(__name__)

# Same keys for expected and actual. cdc_<op> counts events by op in the CDC log.
KEYS = (
    "cdc_r",
    "cdc_c",
    "cdc_u",
    "cdc_d",
    "rejects",
    "duplicate_event_ids",
    "postgres_rows",
)


def expected_from_manifest(manifest: Manifest, snapshot_rows: int) -> dict[str, int]:
    c, u, d = manifest.counts.c, manifest.counts.u, manifest.counts.d
    return {
        "cdc_r": snapshot_rows,
        "cdc_c": c,
        "cdc_u": u,
        "cdc_d": d,
        "rejects": 0,
        "duplicate_event_ids": 0,
        "postgres_rows": snapshot_rows + c - d,
    }


def compare(expected: dict[str, int], actual: dict[str, int]) -> list[str]:
    """Mismatch messages; an empty list means the expectations hold."""
    problems = []
    for key, want in expected.items():
        got = actual.get(key)
        if got is None:
            problems.append(f"{key}: expected {want}, no value collected")
        elif got != want:
            problems.append(f"{key}: expected {want}, got {got}")
    return problems


def format_table(expected: dict[str, int], actual: dict[str, int]) -> str:
    width = max(len(k) for k in expected)
    lines = [f"{'check':<{width}}  {'expected':>8}  {'actual':>8}  ok"]
    for key, want in expected.items():
        got = actual.get(key)
        mark = "yes" if got == want else "NO"
        lines.append(f"{key:<{width}}  {want:>8}  {'-' if got is None else got:>8}  {mark}")
    return "\n".join(lines)


def collect_actuals(
    spark: Any, lake: LakeSettings, pg: PostgresSettings
) -> dict[str, int]:  # spark: SparkSession, untyped to keep this module importable without pyspark
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    actual = dict.fromkeys(KEYS, 0)
    if DeltaTable.isDeltaTable(spark, lake.cdc_log):
        log_df = spark.read.format("delta").load(lake.cdc_log)
        for row in log_df.groupBy("op").count().collect():
            actual[f"cdc_{row['op']}"] = row["count"]
        total = sum(actual[f"cdc_{op}"] for op in "rcud")
        distinct = log_df.select(F.countDistinct("event_id")).first()[0]
        actual["duplicate_event_ids"] = total - distinct
    if DeltaTable.isDeltaTable(spark, lake.rejects):
        actual["rejects"] = spark.read.format("delta").load(lake.rejects).count()
    with psycopg.connect(**pg.connect_kwargs()) as conn:
        actual["postgres_rows"] = count_orders(conn)
    return actual


def poll(
    expected: dict[str, int],
    collect: Callable[[], dict[str, int]],
    timeout: float,
    interval: float = 5.0,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> tuple[dict[str, int], list[str]]:
    deadline = clock() + timeout
    while True:
        actual = collect()
        problems = compare(expected, actual)
        if not problems or clock() >= deadline:
            return actual, problems
        sleep(interval)


def run_verify(
    manifest_path: Path,
    snapshot_rows: int,
    timeout: float,
    lake: LakeSettings,
    pg: PostgresSettings,
    s3: S3Settings | None,
    spark_settings: SparkSettings,
) -> int:
    """Exit code: 0 when everything matches before the timeout, 1 otherwise."""
    from cdc_demo.spark.session import build_session

    manifest = Manifest.model_validate_json(manifest_path.read_text(encoding="utf-8"))
    expected = expected_from_manifest(manifest, snapshot_rows)
    spark = build_session(spark_settings, s3, kafka=False, app_name="cdc-demo-verify")
    try:
        actual, problems = poll(expected, lambda: collect_actuals(spark, lake, pg), timeout)
    finally:
        spark.stop()
    print(format_table(expected, actual))
    if problems:
        print("\nMISMATCH:\n" + "\n".join(problems))
        return 1
    print("\nOK: lake and Postgres match the mutator manifest")
    return 0
