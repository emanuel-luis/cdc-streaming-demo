"""foreachBatch writer for the SQL Server path, and the one-time snapshot.

Same tables as the Kafka path (CDC log, rejects, ingest_runs) under a separate lake root. A
batch carries `txnAppId=<app_id>` and `txnVersion=batch_id`, so a batch Spark replays after a
crash is skipped by Delta. Offsets are dense across the log: a batch starts after the highest
`max_offset` in `ingest_runs`, which is written last, so a replay of a half-written batch
numbers its rows exactly as the first attempt did.
"""

import logging
import uuid
from collections.abc import Callable
from datetime import UTC, datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from cdc_demo.ingest.schemas import INGEST_RUNS_SCHEMA
from cdc_demo.ingest.writer import append_delta
from cdc_demo.logging import log_event
from cdc_demo.mssql.lsn import unpack_lsn
from cdc_demo.mssql.mapping import TOPIC, map_changes
from cdc_demo.settings import LakeSettings

log = logging.getLogger(__name__)

SNAPSHOT_BATCH_ID = -1


def snapshot_app_id(app_id: str) -> str:
    return f"{app_id}#snapshot"


def last_offset(spark: SparkSession, lake: LakeSettings) -> int:
    """Highest offset written so far, -1 when nothing was."""
    row = spark.read.format("delta").load(lake.ingest_runs).agg(F.max("max_offset")).first()
    return -1 if row is None or row[0] is None else int(row[0])


def _write_run(
    spark: SparkSession,
    lake: LakeSettings,
    app_id: str,
    version: int,
    values: tuple[object, ...],
) -> None:
    run = spark.createDataFrame([values], INGEST_RUNS_SCHEMA)
    append_delta(run, lake.ingest_runs, app_id, version, partitioned=False)


class MssqlBatchWriter:
    """Callable for `foreachBatch(writer)`: one batch of `mssql_cdc` rows -> the three tables."""

    def __init__(self, lake: LakeSettings, app_id: str, run_id: str | None = None) -> None:
        self.lake = lake
        self.app_id = app_id
        self.run_id = run_id or uuid.uuid4().hex

    def __call__(self, rows: DataFrame, batch_id: int) -> None:
        started_at = datetime.now(UTC)
        spark = rows.sparkSession
        rows.persist()
        try:
            input_rows = rows.count()
            if input_rows == 0:
                # An idle source still triggers batches: no run row for them (decision 53).
                log_event(log, "batch_empty", run_id=self.run_id, batch_id=batch_id)
                return
            base = last_offset(spark, self.lake)
            parsed = map_changes(rows, batch_id, started_at, base)
            valid, rejects = parsed.valid.persist(), parsed.rejects.persist()
            try:
                valid_rows, rejected_rows = valid.count(), rejects.count()
                if valid_rows:
                    append_delta(valid, self.lake.cdc_log, self.app_id, batch_id)
                if rejected_rows:
                    append_delta(rejects, self.lake.rejects, self.app_id, batch_id)
                emitted = valid_rows + rejected_rows
                _write_run(
                    spark,
                    self.lake,
                    self.app_id,
                    batch_id,
                    (
                        self.run_id,
                        batch_id,
                        started_at,
                        datetime.now(UTC),
                        input_rows,
                        0,
                        valid_rows,
                        rejected_rows,
                        base + 1 if emitted else None,
                        base + emitted if emitted else None,
                        TOPIC,
                    ),
                )
            finally:
                valid.unpersist()
                rejects.unpersist()
            log_event(
                log,
                "batch_written",
                run_id=self.run_id,
                batch_id=batch_id,
                input_rows=input_rows,
                valid_rows=valid_rows,
                rejected_rows=rejected_rows,
            )
        finally:
            rows.unpersist()


def ensure_snapshot(
    spark: SparkSession,
    lake: LakeSettings,
    app_id: str,
    read_snapshot: Callable[[], DataFrame],
    run_id: str | None = None,
) -> str:
    """Make sure the snapshot is in the CDC log and return its LSN (`0x` + 20 hex digits).

    The stream starts after that LSN (`startingLsn`). On a restart the LSN comes back from the
    log itself, and a missing run row (a crash between the two writes) is written again.
    """
    run_id = run_id or uuid.uuid4().hex
    snap_app = snapshot_app_id(app_id)
    reads = spark.read.format("delta").load(lake.cdc_log).where(F.col("op") == "r")
    found = reads.agg(
        F.count(F.lit(1)).alias("n"),
        F.max("source_lsn").alias("lsn"),
        F.min("kafka_offset").alias("lo"),
        F.max("kafka_offset").alias("hi"),
    ).first()
    if found is not None and found["n"]:
        runs = spark.read.format("delta").load(lake.ingest_runs)
        if runs.where(F.col("batch_id") == SNAPSHOT_BATCH_ID).count() == 0:
            now = datetime.now(UTC)
            values = (run_id, SNAPSHOT_BATCH_ID, now, now, found["n"], 0, found["n"], 0)
            _write_run(spark, lake, snap_app, 0, (*values, found["lo"], found["hi"], TOPIC))
            log_event(log, "snapshot_run_recovered", run_id=run_id, rows=found["n"])
        return unpack_lsn(int(found["lsn"]))

    started_at = datetime.now(UTC)
    # One read: every action on the snapshot would otherwise take a new LSN.
    snap = read_snapshot().localCheckpoint(eager=True)
    top = snap.agg(F.max("_start_lsn")).first()
    if top is None or top[0] is None:
        raise RuntimeError("the snapshot returned no rows; the stream cannot start after it")
    start_lsn = str(top[0])
    parsed = map_changes(snap, SNAPSHOT_BATCH_ID, started_at, -1)
    valid, rejects = parsed.valid.persist(), parsed.rejects.persist()
    try:
        valid_rows, rejected_rows = valid.count(), rejects.count()
        if valid_rows:
            append_delta(valid, lake.cdc_log, snap_app, 0)
        if rejected_rows:
            append_delta(rejects, lake.rejects, snap_app, 0)
        emitted = valid_rows + rejected_rows
        _write_run(
            spark,
            lake,
            snap_app,
            0,
            (
                run_id,
                SNAPSHOT_BATCH_ID,
                started_at,
                datetime.now(UTC),
                valid_rows + rejected_rows,
                0,
                valid_rows,
                rejected_rows,
                0 if emitted else None,
                emitted - 1 if emitted else None,
                TOPIC,
            ),
        )
    finally:
        valid.unpersist()
        rejects.unpersist()
    log_event(
        log,
        "snapshot_written",
        run_id=run_id,
        start_lsn=start_lsn,
        rows=valid_rows,
        rejected=rejected_rows,
    )
    return start_lsn
