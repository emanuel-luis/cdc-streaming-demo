"""The SQL Server writer and snapshot, with DataFrames standing in for the `mssql_cdc` source."""

from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession
from test_mssql_mapping import frame, lsn, row

from cdc_demo.ingest.job import create_tables
from cdc_demo.mssql.lsn import pack_lsn
from cdc_demo.mssql.writer import MssqlBatchWriter, ensure_snapshot, last_offset
from cdc_demo.settings import LakeSettings

pytestmark = pytest.mark.spark

APP = "test-mssql-v1"
SNAP_LSN = lsn(5, 0x100)


@pytest.fixture
def lake(spark: SparkSession, tmp_path: Path) -> LakeSettings:
    settings = LakeSettings(lake_root=tmp_path.as_uri())
    create_tables(spark, settings)
    return settings


def table(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.format("delta").load(path)


def snapshot_rows(n: int = 3) -> list[dict[str, object]]:
    return [row(0, SNAP_LSN, None, id=i) for i in range(1, n + 1)]


def test_replayed_batch_is_written_once(spark: SparkSession, lake: LakeSettings) -> None:
    writer = MssqlBatchWriter(lake, APP, run_id="run-1")
    batch = frame(
        spark, [row(2, lsn(5, 0x200), "0x01", id=10), row(2, lsn(5, 0x300), "0x01", id=11)]
    )
    writer(batch, 0)
    writer(batch, 0)  # Spark retrying the batch after a crash
    assert table(spark, lake.cdc_log).count() == 2
    runs = table(spark, lake.ingest_runs).collect()
    assert len(runs) == 1
    assert (runs[0]["input_rows"], runs[0]["valid_rows"], runs[0]["rejected_rows"]) == (2, 2, 0)
    assert (runs[0]["min_offset"], runs[0]["max_offset"]) == (0, 1)
    assert runs[0]["kafka_topic"] == "mssql.dbo.orders"


def test_offsets_continue_across_batches_and_the_snapshot(
    spark: SparkSession, lake: LakeSettings
) -> None:
    ensure_snapshot(spark, lake, APP, lambda: frame(spark, snapshot_rows()), run_id="snap")
    assert last_offset(spark, lake) == 2
    writer = MssqlBatchWriter(lake, APP, run_id="run-1")
    writer(frame(spark, [row(2, lsn(5, 0x200), "0x01", id=10)]), 0)
    writer(frame(spark, []), 1)  # an idle batch writes nothing
    start = lsn(5, 0x300)
    pair = [row(3, start, "0x01", id=1), row(4, start, "0x01", id=1, status="shipped")]
    writer(frame(spark, pair), 2)
    offsets = sorted(r[0] for r in table(spark, lake.cdc_log).select("kafka_offset").collect())
    assert offsets == [0, 1, 2, 3, 4]
    ids = table(spark, lake.cdc_log).select("event_id").distinct().count()
    assert ids == 5
    runs = table(spark, lake.ingest_runs).orderBy("min_offset").collect()
    assert [(r["batch_id"], r["min_offset"], r["max_offset"]) for r in runs] == [
        (-1, 0, 2),
        (0, 3, 3),
        (2, 4, 4),
    ]
    assert runs[2]["input_rows"] == 2  # both rows of the update pair, one event


def test_snapshot_is_written_once_and_returns_its_lsn(
    spark: SparkSession, lake: LakeSettings
) -> None:
    calls: list[int] = []

    def read() -> DataFrame:
        calls.append(1)
        return frame(spark, snapshot_rows())

    first = ensure_snapshot(spark, lake, APP, read, run_id="snap")
    second = ensure_snapshot(spark, lake, APP, read, run_id="snap-2")
    assert first == second == SNAP_LSN
    assert len(calls) == 1
    log = table(spark, lake.cdc_log).collect()
    assert [r["op"] for r in log] == ["r"] * 3
    assert {r["source_lsn"] for r in log} == {pack_lsn(SNAP_LSN)}
    assert table(spark, lake.ingest_runs).count() == 1


def test_snapshot_recovers_a_missing_run_row(
    spark: SparkSession, lake: LakeSettings, tmp_path: Path
) -> None:
    ensure_snapshot(spark, lake, APP, lambda: frame(spark, snapshot_rows()), run_id="snap")
    # A second lake that has the log rows but no run row: a crash between the two writes.
    crashed = LakeSettings(lake_root=(tmp_path / "crashed").as_uri())
    create_tables(spark, crashed)
    table(spark, lake.cdc_log).write.format("delta").mode("append").partitionBy(
        "_partition_date"
    ).save(crashed.cdc_log)
    assert table(spark, crashed.ingest_runs).count() == 0

    def no_read() -> DataFrame:
        raise AssertionError("the snapshot must not be read again")

    assert ensure_snapshot(spark, crashed, APP, no_read, run_id="again") == SNAP_LSN
    runs = table(spark, crashed.ingest_runs).collect()
    assert len(runs) == 1
    assert (runs[0]["batch_id"], runs[0]["min_offset"], runs[0]["max_offset"]) == (-1, 0, 2)
    assert table(spark, crashed.cdc_log).count() == 3


def test_empty_snapshot_stops_the_job(spark: SparkSession, lake: LakeSettings) -> None:
    with pytest.raises(RuntimeError, match="no rows"):
        ensure_snapshot(spark, lake, APP, lambda: frame(spark, []))
