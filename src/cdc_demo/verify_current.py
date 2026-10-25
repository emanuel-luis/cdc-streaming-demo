"""Row-by-row comparison of the current table with Postgres. Pure logic plus two fetchers."""

import logging
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import psycopg

from cdc_demo.settings import LakeSettings, PostgresSettings, S3Settings, SparkSettings

log = logging.getLogger(__name__)

# customer_id, status, amount, created_at, updated_at (timezone-aware UTC)
Fields = tuple[int | None, str | None, Decimal | None, datetime | None, datetime | None]
Rows = dict[int, Fields]
KeyedRows = list[tuple[int, Fields]]  # every row read, duplicates kept

EXAMPLES = 10
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


@dataclass(frozen=True)
class RowDiff:
    current_rows: int
    postgres_rows: int
    missing_in_current: list[int]
    extra_in_current: list[int]
    mismatched: list[int]
    duplicate_keys: list[int]

    @property
    def ok(self) -> bool:
        return not (
            self.missing_in_current
            or self.extra_in_current
            or self.mismatched
            or self.duplicate_keys
        )

    def report(self) -> str:
        lines = [
            f"current_rows        {self.current_rows}",
            f"postgres_rows       {self.postgres_rows}",
            f"missing_in_current  {len(self.missing_in_current)}",
            f"extra_in_current    {len(self.extra_in_current)}",
            f"mismatched_rows     {len(self.mismatched)}",
            f"duplicate_keys      {len(self.duplicate_keys)}",
        ]
        for label, ids in (
            ("missing_in_current", self.missing_in_current),
            ("extra_in_current", self.extra_in_current),
            ("mismatched_rows", self.mismatched),
            ("duplicate_keys", self.duplicate_keys),
        ):
            if ids:
                lines.append(f"{label} examples: {ids[:EXAMPLES]}")
        return "\n".join(lines)


def compare_rows(current: KeyedRows, postgres: Rows) -> RowDiff:
    seen = Counter(k for k, _ in current)
    by_key = dict(current)  # on a duplicate the last row wins; the key already fails the check
    return RowDiff(
        current_rows=len(current),
        postgres_rows=len(postgres),
        missing_in_current=sorted(postgres.keys() - by_key.keys()),
        extra_in_current=sorted(by_key.keys() - postgres.keys()),
        mismatched=sorted(k for k in by_key.keys() & postgres.keys() if by_key[k] != postgres[k]),
        duplicate_keys=sorted(k for k, n in seen.items() if n > 1),
    )


def from_micros(value: int | None) -> datetime | None:
    """Spark `unix_micros` to an aware UTC datetime, independent of the process time zone."""
    return None if value is None else _EPOCH + timedelta(microseconds=value)


def wait_for_match(
    collect: Callable[[], RowDiff],
    timeout: float,
    interval: float = 5.0,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> RowDiff:
    """Re-collect until the diff is empty or the timeout passes; return the last diff."""
    deadline = clock() + timeout
    while True:
        diff = collect()
        if diff.ok or clock() >= deadline:
            return diff
        sleep(interval)


def fetch_postgres(pg: PostgresSettings) -> Rows:
    sql = "SELECT id, customer_id, status, amount, created_at, updated_at FROM orders"
    with psycopg.connect(**pg.connect_kwargs()) as conn, conn.cursor() as cur:
        cur.execute(sql)
        return {
            int(r[0]): (
                r[1],
                r[2],
                r[3],
                r[4].astimezone(UTC) if r[4] else None,
                r[5].astimezone(UTC) if r[5] else None,
            )
            for r in cur.fetchall()
        }


def fetch_current(spark: Any, lake: LakeSettings) -> KeyedRows:
    # ponytail: collects the whole table to the driver; fine at demo scale, compare in Spark
    # (full outer join) if the table grows past a few million rows.
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    if not DeltaTable.isDeltaTable(spark, lake.current_orders):
        return []
    df = spark.read.format("delta").load(lake.current_orders)
    rows = df.select(
        "order_id",
        "customer_id",
        "status",
        "amount",
        F.unix_micros("created_at").alias("created_us"),
        F.unix_micros("updated_at").alias("updated_us"),
    ).collect()
    return [
        (
            r["order_id"],
            (
                r["customer_id"],
                r["status"],
                r["amount"],
                from_micros(r["created_us"]),
                from_micros(r["updated_us"]),
            ),
        )
        for r in rows
    ]


def run_verify_current(
    timeout: float,
    lake: LakeSettings,
    pg: PostgresSettings,
    s3: S3Settings | None,
    spark_settings: SparkSettings,
) -> int:
    """Exit code: 0 when the current table equals Postgres before the timeout, 1 otherwise."""
    from cdc_demo.spark.session import build_session

    spark = build_session(spark_settings, s3, kafka=False, app_name="cdc-demo-verify-current")
    try:
        diff = wait_for_match(
            lambda: compare_rows(fetch_current(spark, lake), fetch_postgres(pg)), timeout
        )
    finally:
        spark.stop()
    print(diff.report())
    if diff.ok:
        print("\nOK: the current table matches Postgres row by row")
        return 0
    print("\nMISMATCH: the current table differs from Postgres")
    return 1
