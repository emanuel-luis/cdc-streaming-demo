from pathlib import Path

import pytest
from protobuf_support import fixture_event, kafka_value
from pyspark.sql import SparkSession

from cdc_demo.ingest.writer import BatchWriter
from cdc_demo.settings import LakeSettings

pytestmark = pytest.mark.spark

TOPIC = "cdc.public.orders"


def value_of(name: str) -> bytes:
    return kafka_value(fixture_event(name))


def make_raw(spark: SparkSession, values: list[bytes | None], first_offset: int = 0):  # type: ignore[no-untyped-def]
    from datetime import UTC, datetime

    now = datetime(2026, 10, 26, tzinfo=UTC)
    rows = [(None, v, TOPIC, 0, first_offset + i, now) for i, v in enumerate(values)]
    return spark.createDataFrame(
        rows,
        "key binary, value binary, topic string, partition int, offset long, timestamp timestamp",
    )


def test_batch_written_once_even_when_retried(spark: SparkSession, tmp_path: Path) -> None:
    lake = LakeSettings(lake_root=tmp_path.as_uri())
    writer = BatchWriter(lake, "test-app-v1", TOPIC, run_id="run-1")
    values = [value_of("create"), value_of("update"), None, b"{broken"]
    raw = make_raw(spark, values)

    writer(raw, 0)
    writer(raw, 0)  # Spark retrying the same batch after a crash

    def count(path: str) -> int:
        return spark.read.format("delta").load(path).count()

    assert count(lake.cdc_log) == 2
    assert count(lake.rejects) == 1
    runs = spark.read.format("delta").load(lake.ingest_runs).collect()
    assert len(runs) == 1
    run = runs[0]
    assert (run["input_rows"], run["tombstones"], run["valid_rows"], run["rejected_rows"]) == (
        4,
        1,
        2,
        1,
    )
    assert (run["min_offset"], run["max_offset"], run["run_id"]) == (0, 3, "run-1")

    # A new batch id with new offsets is appended normally.
    writer(make_raw(spark, [value_of("delete")], first_offset=4), 1)
    assert count(lake.cdc_log) == 3
    assert count(lake.ingest_runs) == 2
    ids = spark.read.format("delta").load(lake.cdc_log).select("event_id").distinct().count()
    assert ids == 3
