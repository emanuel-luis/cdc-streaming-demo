"""The SQL Server consumer: snapshot once, then `mssql_cdc` streaming into the CDC log."""

import logging

from cdc_demo.ingest.job import create_tables
from cdc_demo.logging import log_event
from cdc_demo.mssql.writer import MssqlBatchWriter, ensure_snapshot
from cdc_demo.settings import (
    ConsumerSettings,
    LakeSettings,
    S3Settings,
    SparkSettings,
    SqlServerSettings,
)
from cdc_demo.spark.session import build_session

log = logging.getLogger(__name__)

QUERY_NAME = "orders_mssql_cdc_log"


def run_mssql_consumer(
    *,
    sql: SqlServerSettings,
    lake: LakeSettings,
    consumer: ConsumerSettings,
    spark_settings: SparkSettings,
    s3: S3Settings | None,
    available_now: bool = False,
) -> None:
    from mssql_cdc import register

    spark = build_session(spark_settings, s3, kafka=False, app_name="cdc-demo-consume-mssql")
    register(spark)
    create_tables(spark, lake)
    options = sql.source_options()
    start_lsn = ensure_snapshot(
        spark,
        lake,
        consumer.app_id,
        lambda: spark.read.format("mssql_cdc_snapshot").options(**options).load(),
    )
    writer = MssqlBatchWriter(lake, consumer.app_id)
    # A checkpoint that has offsets ignores startingLsn, so a restart resumes where it stopped.
    stream = (
        spark.readStream.format("mssql_cdc")
        .options(**options)
        .option("startingLsn", start_lsn)
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
        source="sqlserver",
        start_lsn=start_lsn,
        available_now=available_now,
    )
    query = out.start()
    try:
        query.awaitTermination()
    finally:
        spark.stop()
