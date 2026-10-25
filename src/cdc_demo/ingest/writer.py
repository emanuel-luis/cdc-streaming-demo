"""foreachBatch writer: one batch -> CDC log + rejects + one ingest_runs row, idempotently."""

import logging
import uuid
from datetime import UTC, datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from cdc_demo.ingest.parse import parse_batch
from cdc_demo.ingest.schemas import INGEST_RUNS_SCHEMA
from cdc_demo.logging import log_event
from cdc_demo.settings import LakeSettings

log = logging.getLogger(__name__)


def append_delta(
    df: DataFrame, path: str, app_id: str, version: int, partitioned: bool = True
) -> None:
    """Append to a Delta table, once per (app_id, version): Delta skips a repeated pair."""
    writer = (
        df.write.format("delta")
        .mode("append")
        .option("txnAppId", app_id)
        .option("txnVersion", version)
    )
    if partitioned:
        writer = writer.partitionBy("_partition_date")
    writer.save(path)


class BatchWriter:
    """Callable for `foreachBatch(writer)`.

    Every Delta write carries txnAppId + txnVersion=batch_id, so Delta skips a batch that was
    already committed to that table when Spark retries it after a crash. The batch id is only
    stable while the checkpoint is kept: wiping the checkpoint without the tables would make
    Delta skip the new batches that reuse old ids.
    """

    def __init__(
        self, lake: LakeSettings, app_id: str, topic: str, run_id: str | None = None
    ) -> None:
        self.lake = lake
        self.app_id = app_id
        self.topic = topic
        self.run_id = run_id or uuid.uuid4().hex

    def __call__(self, raw: DataFrame, batch_id: int) -> None:
        started_at = datetime.now(UTC)
        raw.persist()
        try:
            parsed = parse_batch(raw, batch_id, started_at)
            valid, rejects = parsed.valid.persist(), parsed.rejects.persist()
            try:
                stats = raw.agg(
                    F.count(F.lit(1)).alias("n"),
                    F.min("offset").alias("lo"),
                    F.max("offset").alias("hi"),
                ).first()
                if stats is None:
                    raise RuntimeError("aggregate over the batch returned no row")
                valid_rows, rejected_rows = valid.count(), rejects.count()
                if valid_rows:
                    append_delta(valid, self.lake.cdc_log, self.app_id, batch_id)
                if rejected_rows:
                    append_delta(rejects, self.lake.rejects, self.app_id, batch_id)
                run = raw.sparkSession.createDataFrame(
                    [
                        (
                            self.run_id,
                            batch_id,
                            started_at,
                            datetime.now(UTC),
                            stats["n"],
                            parsed.tombstones,
                            valid_rows,
                            rejected_rows,
                            stats["lo"],
                            stats["hi"],
                            self.topic,
                        )
                    ],
                    INGEST_RUNS_SCHEMA,
                )
                append_delta(run, self.lake.ingest_runs, self.app_id, batch_id, partitioned=False)
            finally:
                valid.unpersist()
                rejects.unpersist()
            log_event(
                log,
                "batch_written",
                run_id=self.run_id,
                batch_id=batch_id,
                input_rows=stats["n"],
                tombstones=parsed.tombstones,
                valid_rows=valid_rows,
                rejected_rows=rejected_rows,
            )
        finally:
            raw.unpersist()
