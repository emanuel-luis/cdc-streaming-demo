from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import SparkSession

from cdc_demo.ingest.schemas import CDC_LOG_SCHEMA
from cdc_demo.merge import job
from cdc_demo.merge.job import MergeResult, run_merge
from cdc_demo.settings import LakeSettings

pytestmark = pytest.mark.spark

TOPIC = "cdc.public.orders"
T0 = datetime(2026, 10, 25, 12, 0, tzinfo=UTC)


def event(
    order_id: int,
    op: str,
    lsn: int,
    offset: int,
    status: str = "pending",
    amount: str = "10.00",
    partition: int = 0,
) -> dict[str, Any]:
    gone = op == "d"
    return {
        "event_id": f"{TOPIC}-{partition}-{offset}",
        "op": op,
        "order_id": order_id,
        "customer_id": None if gone else 1,
        "status": None if gone else status,
        "amount": None if gone else Decimal(amount),
        "created_at": None if gone else T0,
        "updated_at": None if gone else T0,
        "source_lsn": lsn,
        "kafka_topic": TOPIC,
        "kafka_partition": partition,
        "kafka_offset": offset,
        "ingested_at": T0,
    }


def append_log(spark: SparkSession, lake: LakeSettings, events: list[dict[str, Any]]) -> None:
    names = CDC_LOG_SCHEMA.fieldNames()
    rows = [tuple(e.get(n) for n in names) for e in events]
    df = spark.createDataFrame(rows, CDC_LOG_SCHEMA)
    df.write.format("delta").mode("append").save(lake.cdc_log)


def current(spark: SparkSession, lake: LakeSettings) -> dict[int, tuple[Any, ...]]:
    rows = spark.read.format("delta").load(lake.current_orders).collect()
    return {r["order_id"]: (r["status"], r["amount"], r["_op"], r["_kafka_offset"]) for r in rows}


def watermark(spark: SparkSession, lake: LakeSettings) -> dict[int, int]:
    rows = spark.read.format("delta").load(lake.merge_watermark).collect()
    return {r["kafka_partition"]: r["last_offset"] for r in rows}


def runs(spark: SparkSession, lake: LakeSettings) -> list[Any]:
    df = spark.read.format("delta").load(lake.merge_runs).orderBy("started_at")
    return df.collect()


def counts(r: MergeResult) -> tuple[int, int, int, int, int]:
    return (r.events_read, r.distinct_orders, r.rows_inserted, r.rows_updated, r.rows_deleted)


@pytest.fixture
def lake(tmp_path: Path) -> LakeSettings:
    return LakeSettings(lake_root=tmp_path.as_uri())


def test_dedup_picks_latest_lsn_and_breaks_ties_by_offset(
    spark: SparkSession, lake: LakeSettings
) -> None:
    append_log(
        spark,
        lake,
        [
            event(1, "c", lsn=10, offset=0, status="pending"),
            event(1, "u", lsn=30, offset=2, status="shipped"),
            event(1, "u", lsn=20, offset=1, status="confirmed"),
            # Same LSN: the higher offset wins, whatever the row order in the log.
            event(2, "u", lsn=50, offset=5, status="confirmed"),
            event(2, "u", lsn=50, offset=4, status="pending"),
        ],
    )
    result = run_merge(spark, lake)
    assert counts(result) == (5, 2, 2, 0, 0)
    table = current(spark, lake)
    assert table[1][0] == "shipped"
    assert table[2][0] == "confirmed"


def test_insert_update_delete(spark: SparkSession, lake: LakeSettings) -> None:
    append_log(
        spark,
        lake,
        [
            event(1, "r", lsn=1, offset=0),
            event(2, "r", lsn=1, offset=1),
            event(3, "r", lsn=1, offset=2),
            event(4, "c", lsn=2, offset=3),
            event(4, "d", lsn=3, offset=4),  # created and deleted before the first run
        ],
    )
    assert counts(run_merge(spark, lake)) == (5, 4, 3, 0, 0)
    assert set(current(spark, lake)) == {1, 2, 3}

    append_log(
        spark,
        lake,
        [
            event(1, "u", lsn=10, offset=5, status="shipped", amount="12.50"),
            event(2, "d", lsn=11, offset=6),
            event(5, "c", lsn=12, offset=7),
        ],
    )
    assert counts(run_merge(spark, lake)) == (3, 3, 1, 1, 1)
    table = current(spark, lake)
    assert set(table) == {1, 3, 5}
    assert table[1] == ("shipped", Decimal("12.50"), "u", 5)
    assert table[3][2] == "r"


def test_older_event_does_not_regress_a_row(spark: SparkSession, lake: LakeSettings) -> None:
    append_log(spark, lake, [event(1, "u", lsn=30, offset=2, status="shipped")])
    run_merge(spark, lake)
    # A later-ingested event with an older LSN (e.g. a replay) must be ignored.
    append_log(spark, lake, [event(1, "u", lsn=20, offset=3, status="pending")])
    result = run_merge(spark, lake)
    assert counts(result) == (1, 1, 0, 0, 0)
    assert current(spark, lake)[1][0] == "shipped"


def test_rerun_without_new_events_changes_nothing(spark: SparkSession, lake: LakeSettings) -> None:
    append_log(spark, lake, [event(1, "r", lsn=1, offset=0), event(2, "r", lsn=1, offset=1)])
    run_merge(spark, lake)
    table, wm = current(spark, lake), watermark(spark, lake)
    version = spark.sql(f"DESCRIBE HISTORY delta.`{lake.current_orders}`").count()

    second = run_merge(spark, lake)

    assert counts(second) == (0, 0, 0, 0, 0)
    assert current(spark, lake) == table
    assert watermark(spark, lake) == wm == {0: 1}
    assert spark.sql(f"DESCRIBE HISTORY delta.`{lake.current_orders}`").count() == version
    assert second.watermark_before == second.watermark_after


def test_incremental_run_reads_only_offsets_above_the_watermark(
    spark: SparkSession, lake: LakeSettings
) -> None:
    append_log(spark, lake, [event(i, "r", lsn=1, offset=i - 1) for i in (1, 2, 3)])
    first = run_merge(spark, lake)
    assert first.events_read == 3
    assert watermark(spark, lake) == {0: 2}

    append_log(spark, lake, [event(1, "u", lsn=5, offset=3, status="shipped"), event(9, "c", 6, 4)])
    second = run_merge(spark, lake)
    assert counts(second) == (2, 2, 1, 1, 0)
    assert watermark(spark, lake) == {0: 4}
    assert second.watermark_before == f'{{"{TOPIC}:0": 2}}'
    assert second.watermark_after == f'{{"{TOPIC}:0": 4}}'


def test_watermark_is_tracked_per_partition(spark: SparkSession, lake: LakeSettings) -> None:
    append_log(
        spark,
        lake,
        [event(1, "r", 1, 5, partition=0), event(2, "r", 1, 0, partition=1)],
    )
    run_merge(spark, lake)
    assert watermark(spark, lake) == {0: 5, 1: 0}
    append_log(spark, lake, [event(3, "r", 2, 1, partition=1)])
    assert counts(run_merge(spark, lake)) == (1, 1, 1, 0, 0)


def test_crash_between_merge_and_watermark_is_harmless(
    spark: SparkSession, lake: LakeSettings, monkeypatch: pytest.MonkeyPatch
) -> None:
    append_log(
        spark,
        lake,
        [
            event(1, "r", 1, 0),
            event(2, "r", 1, 1),
            event(1, "u", 5, 2, status="shipped"),
            event(2, "d", 6, 3),
            event(3, "c", 7, 4),
        ],
    )

    def boom(*_: Any, **__: Any) -> None:
        raise RuntimeError("crash after the MERGE committed")

    with monkeypatch.context() as m:
        m.setattr(job, "_advance_watermark", boom)
        with pytest.raises(RuntimeError, match="crash"):
            run_merge(spark, lake)
    expected = current(spark, lake)
    assert set(expected) == {1, 3}
    assert watermark(spark, lake) == {}

    result = run_merge(spark, lake)

    assert counts(result) == (5, 3, 0, 0, 0)
    assert current(spark, lake) == expected
    assert watermark(spark, lake) == {0: 4}


def test_merge_run_records_the_pinned_version(spark: SparkSession, lake: LakeSettings) -> None:
    append_log(spark, lake, [event(1, "r", 1, 0)])
    append_log(spark, lake, [event(2, "r", 1, 1)])
    result = run_merge(spark, lake, run_id="run-1")
    assert result.cdc_log_version == 1  # two appends: versions 0 and 1

    append_log(spark, lake, [event(3, "r", 1, 2)])
    run_merge(spark, lake, run_id="run-2")

    first, second = runs(spark, lake)
    assert (first["run_id"], first["cdc_log_version"], first["events_read"]) == ("run-1", 1, 2)
    assert (second["run_id"], second["cdc_log_version"], second["rows_inserted"]) == (
        "run-2",
        2,
        1,
    )
    assert first["rows_inserted"] == 2
    assert first["finished_at"] >= first["started_at"]


def test_empty_cdc_log(spark: SparkSession, lake: LakeSettings) -> None:
    append_log(spark, lake, [])
    result = run_merge(spark, lake)
    assert counts(result) == (0, 0, 0, 0, 0)
    assert result.cdc_log_version == 0
    assert current(spark, lake) == {}


def test_missing_cdc_log(spark: SparkSession, lake: LakeSettings) -> None:
    result = run_merge(spark, lake)
    assert counts(result) == (0, 0, 0, 0, 0)
    assert result.cdc_log_version is None
    (only,) = runs(spark, lake)
    assert only["cdc_log_version"] is None
    assert current(spark, lake) == {}

    # The CDC log appears later; the same code path merges it.
    append_log(spark, lake, [event(1, "r", 1, 0)])
    assert counts(run_merge(spark, lake)) == (1, 1, 1, 0, 0)


def test_merge_ignores_cdc_log_versions_after_the_pinned_read(
    spark: SparkSession, lake: LakeSettings, monkeypatch: pytest.MonkeyPatch
) -> None:
    append_log(spark, lake, [event(1, "r", 1, 0)])  # version 0
    append_log(spark, lake, [event(2, "r", 1, 1)])  # version 1, appended "during" the run
    real = job._latest_version

    def pinned_early(s: SparkSession, path: str) -> int:
        return 0 if path == lake.cdc_log else real(s, path)

    with monkeypatch.context() as m:
        m.setattr(job, "_latest_version", pinned_early)
        first = run_merge(spark, lake)
    assert first.cdc_log_version == 0
    assert counts(first) == (1, 1, 1, 0, 0)
    assert set(current(spark, lake)) == {1}
    assert watermark(spark, lake) == {0: 0}

    second = run_merge(spark, lake)
    assert second.cdc_log_version == 1
    assert counts(second) == (1, 1, 1, 0, 0)
    assert set(current(spark, lake)) == {1, 2}
    assert watermark(spark, lake) == {0: 1}


def test_order_deleted_in_one_run_can_be_reinserted_in_the_next(
    spark: SparkSession, lake: LakeSettings
) -> None:
    append_log(spark, lake, [event(1, "c", lsn=1, offset=0)])
    assert counts(run_merge(spark, lake)) == (1, 1, 1, 0, 0)
    assert set(current(spark, lake)) == {1}

    # The row is in the table, so this MERGE really deletes it.
    append_log(spark, lake, [event(1, "d", lsn=2, offset=1)])
    assert counts(run_merge(spark, lake)) == (1, 1, 0, 0, 1)
    assert current(spark, lake) == {}

    append_log(spark, lake, [event(1, "c", lsn=5, offset=2, status="confirmed", amount="20.00")])
    assert counts(run_merge(spark, lake)) == (1, 1, 1, 0, 0)
    assert current(spark, lake) == {1: ("confirmed", Decimal("20.00"), "c", 2)}
