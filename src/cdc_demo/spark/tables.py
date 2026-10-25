"""Create Delta tables from explicit schemas, so an empty table reads and merges like any other."""

from collections.abc import Sequence

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType


def create_table(
    spark: SparkSession, path: str, schema: StructType, partition_by: Sequence[str] = ()
) -> None:
    builder = DeltaTable.createIfNotExists(spark).location(qualified_local(path))
    builder = builder.addColumns(schema)
    if partition_by:
        builder = builder.partitionedBy(*partition_by)
    builder.execute()


def qualified_local(path: str) -> str:
    """`file:///x` as Hadoop qualifies it (`file:/x`).

    A path-based CREATE TABLE fails with DELTA_AMBIGUOUS_PATHS_IN_CREATE_TABLE unless the
    qualified location ends with the path as given, and `file:/x` does not end with `file:///x`.
    `s3a://` and plain paths already qualify to a string that ends with themselves.
    """
    prefix = "file:///"
    return "file:/" + path.removeprefix(prefix) if path.startswith(prefix) else path
