import pytest


def test_spark_packages_match_installed_pyspark() -> None:
    pyspark = pytest.importorskip("pyspark")
    pytest.importorskip("delta")
    from cdc_demo.spark.session import hadoop_version, spark_packages

    kafka, aws, protobuf = spark_packages()
    assert kafka == f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}"
    assert aws == f"org.apache.hadoop:hadoop-aws:{hadoop_version()}"
    assert protobuf == f"org.apache.spark:spark-protobuf_2.13:{pyspark.__version__}"
    assert hadoop_version().count(".") == 2
