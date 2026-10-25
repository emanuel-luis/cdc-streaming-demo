"""The Structured Streaming query: Kafka -> parse -> Delta."""

import logging

from pyspark.sql import SparkSession

from cdc_demo.ingest.schemas import CDC_LOG_SCHEMA, INGEST_RUNS_SCHEMA, REJECTS_SCHEMA
from cdc_demo.ingest.writer import BatchWriter
from cdc_demo.logging import log_event
from cdc_demo.settings import (
    ConsumerSettings,
    KafkaSettings,
    LakeSettings,
    S3Settings,
    SparkSettings,
)
from cdc_demo.spark.session import build_session
from cdc_demo.spark.tables import create_table

log = logging.getLogger(__name__)

QUERY_NAME = "orders_cdc_log"


def create_tables(spark: SparkSession, lake: LakeSettings) -> None:
    """Decision 36: the rejects table would otherwise exist only after the first reject."""
    create_table(spark, lake.cdc_log, CDC_LOG_SCHEMA, ["_partition_date"])
    create_table(spark, lake.rejects, REJECTS_SCHEMA, ["_partition_date"])
    create_table(spark, lake.ingest_runs, INGEST_RUNS_SCHEMA)


def run_consumer(
    *,
    kafka: KafkaSettings,
    lake: LakeSettings,
    consumer: ConsumerSettings,
    spark_settings: SparkSettings,
    s3: S3Settings | None,
    available_now: bool = False,
) -> None:
    spark = build_session(spark_settings, s3, kafka=True, app_name="cdc-demo-consume")
    create_tables(spark, lake)
    writer = BatchWriter(lake, consumer.app_id, kafka.cdc_topic)
    stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka.kafka_bootstrap_servers)
        .option("subscribe", kafka.cdc_topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", consumer.max_offsets_per_trigger)
        .option("failOnDataLoss", "true")
        .load()
    )
    out = (
        stream.writeStream.foreachBatch(writer)
        .option("checkpointLocation", lake.checkpoint(QUERY_NAME))
        .queryName(QUERY_NAME)
    )
    out = (
        out.trigger(availableNow=True)
        if available_now
        else out.trigger(processingTime=consumer.trigger_interval)
    )
    log_event(
        log,
        "consumer_start",
        run_id=writer.run_id,
        topic=kafka.cdc_topic,
        available_now=available_now,
    )
    query = out.start()
    try:
        query.awaitTermination()
    finally:
        spark.stop()
