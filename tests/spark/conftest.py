from collections.abc import Iterator

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from pyspark.sql import SparkSession

from cdc_demo.settings import SparkSettings
from cdc_demo.spark.session import build_session


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    # No Kafka, no S3: only Delta is resolved, and the lake is a local directory.
    session = build_session(
        SparkSettings(spark_master="local[1]", spark_driver_memory="1g"),
        kafka=False,
        app_name="cdc-demo-tests",
    )
    session.conf.set("spark.sql.shuffle.partitions", "1")
    yield session
    session.stop()
