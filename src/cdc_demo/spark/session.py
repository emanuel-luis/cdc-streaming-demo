"""SparkSession builder: Delta, Kafka, Protobuf, s3a, UTC."""

import re
from pathlib import Path
from urllib.parse import urlparse

import pyspark
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from cdc_demo.settings import S3Settings, SparkSettings

SCALA_BINARY = "2.13"
_HADOOP_JAR = re.compile(r"hadoop-client-api-(\d+\.\d+\.\d+)\.jar")


def hadoop_version() -> str:
    """Hadoop version bundled in the installed pyspark, read from its jar file name."""
    jars = Path(pyspark.__file__).parent / "jars"
    for jar in sorted(jars.glob("hadoop-client-api-*.jar")):
        match = _HADOOP_JAR.fullmatch(jar.name)
        if match:
            return match.group(1)
    raise RuntimeError(f"no hadoop-client-api-<version>.jar found in {jars}")


def spark_packages() -> list[str]:
    """Maven coordinates (besides Delta) that must match the installed pyspark."""
    return [
        f"org.apache.spark:spark-sql-kafka-0-10_{SCALA_BINARY}:{pyspark.__version__}",
        f"org.apache.hadoop:hadoop-aws:{hadoop_version()}",
        f"org.apache.spark:spark-protobuf_{SCALA_BINARY}:{pyspark.__version__}",
    ]


def build_session(
    spark: SparkSettings,
    s3: S3Settings | None = None,
    *,
    kafka: bool = True,
    protobuf: bool | None = None,
    hadoop_aws: bool | None = None,
    app_name: str = "cdc-demo",
) -> SparkSession:
    """Build a Delta-enabled session.

    Kafka, spark-protobuf and hadoop-aws jars are only requested when needed, so tests with a
    local file://lake root resolve little besides Delta. protobuf defaults to the kafka flag
    and hadoop-aws to on when S3 settings are given.
    """
    kafka_pkg, aws_pkg, protobuf_pkg = spark_packages()
    if protobuf is None:
        protobuf = kafka
    if hadoop_aws is None:
        hadoop_aws = s3 is not None
    extra = (
        ([kafka_pkg] if kafka else [])
        + ([protobuf_pkg] if protobuf else [])
        + ([aws_pkg] if hadoop_aws else [])
    )

    builder = (
        SparkSession.builder.appName(app_name)
        .master(spark.spark_master)
        .config("spark.driver.memory", spark.spark_driver_memory)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
    )
    if spark.spark_ivy_dir:
        builder = builder.config("spark.jars.ivy", spark.spark_ivy_dir)
    if s3 is not None:
        builder = _configure_s3(builder, s3)
    return configure_spark_with_delta_pip(builder, extra_packages=extra).getOrCreate()


def _configure_s3(builder: SparkSession.Builder, s3: S3Settings) -> SparkSession.Builder:
    return (
        builder.config("spark.hadoop.fs.s3a.endpoint", s3.endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.access.key", s3.access_key)
        .config("spark.hadoop.fs.s3a.secret.key", s3.secret_key.get_secret_value())
        .config("spark.hadoop.fs.s3a.endpoint.region", s3.region)
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            str(urlparse(s3.endpoint).scheme == "https").lower(),
        )
    )
