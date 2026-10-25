from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from test_writer import TOPIC, make_raw, value_of

from cdc_demo.ingest.job import create_tables
from cdc_demo.ingest.schemas import CDC_LOG_SCHEMA, INGEST_RUNS_SCHEMA, REJECTS_SCHEMA
from cdc_demo.ingest.writer import BatchWriter
from cdc_demo.settings import LakeSettings

pytestmark = pytest.mark.spark


@pytest.fixture
def lake(tmp_path: Path) -> LakeSettings:
    return LakeSettings(lake_root=tmp_path.as_uri())


def read(spark: SparkSession, path: str):  # type: ignore[no-untyped-def]
    return spark.read.format("delta").load(path)


def test_tables_exist_empty_before_any_batch(spark: SparkSession, lake: LakeSettings) -> None:
    create_tables(spark, lake)
    create_tables(spark, lake)  # a restart must not fail or change anything
    for path, schema in (
        (lake.cdc_log, CDC_LOG_SCHEMA),
        (lake.rejects, REJECTS_SCHEMA),
        (lake.ingest_runs, INGEST_RUNS_SCHEMA),
    ):
        df = read(spark, path)
        assert df.count() == 0
        assert [f.name for f in df.schema.fields] == schema.fieldNames()


def test_partitioning_matches_the_writer(spark: SparkSession, lake: LakeSettings) -> None:
    create_tables(spark, lake)
    for path in (lake.cdc_log, lake.rejects):
        detail = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").first()
        assert detail is not None
        assert list(detail["partitionColumns"]) == ["_partition_date"]


def test_writer_appends_to_the_created_tables(spark: SparkSession, lake: LakeSettings) -> None:
    create_tables(spark, lake)
    writer = BatchWriter(lake, "test-app-v1", TOPIC, run_id="run-1")
    writer(make_raw(spark, [value_of("create"), b"{broken"]), 0)
    assert read(spark, lake.cdc_log).count() == 1
    assert read(spark, lake.rejects).count() == 1
    assert read(spark, lake.ingest_runs).count() == 1
