"""Fixture rows are shaped like the documented output schema of mssql-cdc-pyspark
(docs/reference/output-schema.md); a live run of `make mssql-demo` catches any drift."""

import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
)

from cdc_demo.ingest.parse import ParsedBatch
from cdc_demo.merge.job import run_merge
from cdc_demo.mssql.lsn import pack_lsn
from cdc_demo.mssql.mapping import map_changes
from cdc_demo.settings import LakeSettings

pytestmark = pytest.mark.spark

SOURCE_SCHEMA = StructType(
    [
        StructField("_capture_instance", StringType()),
        StructField("_start_lsn", StringType()),
        StructField("_seqval", StringType()),
        StructField("_operation", IntegerType()),
        StructField("_command_id", IntegerType()),
        StructField("_commit_ts", TimestampNTZType()),
        StructField("id", LongType()),
        StructField("customer_id", IntegerType()),
        StructField("status", StringType()),
        StructField("amount", DecimalType(12, 2)),
        StructField("created_at", TimestampNTZType()),
        StructField("updated_at", TimestampNTZType()),
    ]
)
COMMIT = datetime(2026, 10, 25, 12, 0, 0, 500_000)  # naive: the source's UTC wall time
COMMIT_UTC = COMMIT.replace(tzinfo=UTC)
INGESTED = datetime(2026, 10, 25, 12, 5, tzinfo=UTC)


def lsn(vlf: int, block: int, slot: int = 1) -> str:
    return f"0x{vlf:08X}{block:08X}{slot:04X}"


def row(op: int, start: str, seq: str | None, **kw: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "_capture_instance": "dbo_orders",
        "_start_lsn": start,
        "_seqval": seq,
        "_operation": op,
        "_command_id": None if op == 0 else 1,
        "_commit_ts": COMMIT,
        "id": 1,
        "customer_id": 7,
        "status": "pending",
        "amount": Decimal("10.00"),
        "created_at": COMMIT,
        "updated_at": COMMIT,
    }
    return base | kw


def frame(
    spark: SparkSession, rows: list[dict[str, Any]], schema: StructType = SOURCE_SCHEMA
) -> DataFrame:
    return spark.createDataFrame([tuple(r.get(n) for n in schema.names) for r in rows], schema)


def mapped(
    spark: SparkSession, rows: list[dict[str, Any]], base: int = -1, batch: int = 0
) -> ParsedBatch:
    return map_changes(frame(spark, rows), batch, INGESTED, base)


def events(df: DataFrame) -> list[Any]:
    return df.orderBy("kafka_offset").collect()


def append_log(df: DataFrame, lake: LakeSettings) -> None:
    df.write.format("delta").mode("append").partitionBy("_partition_date").save(lake.cdc_log)


def test_snapshot_rows_become_reads(spark: SparkSession) -> None:
    start = lsn(5, 0x100)
    rows = [row(0, start, None, id=i, customer_id=i) for i in (3, 1, 2)]
    out = mapped(spark, rows, base=-1, batch=-1)
    valid = events(out.valid)
    assert [e["op"] for e in valid] == ["r", "r", "r"]
    assert [e["order_id"] for e in valid] == [1, 2, 3]  # `id` breaks the snapshot's ties
    assert [e["kafka_offset"] for e in valid] == [0, 1, 2]
    assert valid[0]["event_id"] == "mssql.dbo.orders-0-0"
    assert {e["source_lsn"] for e in valid} == {pack_lsn(start)}
    assert valid[0]["before_status"] is None and valid[0]["before_amount"] is None
    assert valid[0]["ingest_batch_id"] == -1
    assert valid[0]["kafka_topic"] == "mssql.dbo.orders" and valid[0]["kafka_partition"] == 0
    assert out.rejects.count() == 0 and out.tombstones == 0


def test_insert_keeps_the_after_image_and_commit_time(spark: SparkSession) -> None:
    out = mapped(spark, [row(2, lsn(5, 0x200), "0x01", id=9, status="pending")])
    (e,) = events(out.valid)
    assert (e["op"], e["order_id"], e["status"]) == ("c", 9, "pending")
    assert e["amount"] == Decimal("10.00")
    assert e["source_ts_ms"] == int(COMMIT_UTC.timestamp() * 1000)
    assert e["created_at"] == COMMIT_UTC
    assert e["kafka_timestamp"] == COMMIT_UTC
    assert e["_partition_date"].isoformat() == "2026-10-25"
    assert e["source_tx_id"] is None and e["ingested_at"] == INGESTED


def test_update_pair_is_one_event_with_the_before_image(spark: SparkSession) -> None:
    start, seq = lsn(5, 0x300), "0x0A"
    rows = [  # the after row listed first: pairing must not depend on row order
        row(4, start, seq, status="confirmed", amount=Decimal("12.50")),
        row(3, start, seq, status="pending", amount=Decimal("10.00")),
    ]
    out = mapped(spark, rows)
    (e,) = events(out.valid)
    assert (e["op"], e["status"], e["amount"]) == ("u", "confirmed", Decimal("12.50"))
    assert (e["before_status"], e["before_amount"]) == ("pending", Decimal("10.00"))
    assert out.rejects.count() == 0


def test_delete_has_null_after_columns_and_its_values_as_before(spark: SparkSession) -> None:
    out = mapped(spark, [row(1, lsn(5, 0x400), "0x01", id=4, status="cancelled")])
    (e,) = events(out.valid)
    assert (e["op"], e["order_id"]) == ("d", 4)
    after = ("customer_id", "status", "amount", "created_at", "updated_at")
    assert all(e[c] is None for c in after)
    assert (e["before_status"], e["before_amount"]) == ("cancelled", Decimal("10.00"))


def test_two_updates_in_one_transaction_keep_statement_order(spark: SparkSession) -> None:
    start = lsn(5, 0x500)
    rows = [
        row(4, start, "0x02", _command_id=2, status="shipped"),
        row(3, start, "0x02", _command_id=2, status="confirmed"),
        row(4, start, "0x01", _command_id=1, status="confirmed"),
        row(3, start, "0x01", _command_id=1, status="pending"),
    ]
    out = mapped(spark, rows, base=9)
    valid = events(out.valid)
    assert [(e["status"], e["before_status"]) for e in valid] == [
        ("confirmed", "pending"),
        ("shipped", "confirmed"),
    ]
    assert [e["kafka_offset"] for e in valid] == [10, 11]
    assert len({e["source_lsn"] for e in valid}) == 1


def test_unpaired_update_rows_are_rejected(spark: SparkSession) -> None:
    rows = [
        row(3, lsn(5, 0x600), "0x01", id=1),  # orphan before image
        row(4, lsn(5, 0x700), "0x01", id=2),  # orphan after image
        row(2, lsn(5, 0x800), "0x01", id=3),
    ]
    out = mapped(spark, rows)
    valid = events(out.valid)
    assert [e["order_id"] for e in valid] == [3]
    rejects = events(out.rejects)
    assert [r["reason"] for r in rejects] == ["unpaired_update", "unpaired_update"]
    assert [json.loads(r["raw_value"])["id"] for r in rejects] == [1, 2]
    # Offsets stay dense across valid rows and rejects.
    assert [r["kafka_offset"] for r in rejects] == [0, 1]
    assert valid[0]["kafka_offset"] == 2


def test_lsn_out_of_range_and_other_rejects(spark: SparkSession) -> None:
    rows = [
        row(2, lsn(1 << 23, 1), "0x01", id=1),  # vlf does not fit
        row(2, lsn(1, 1 << 24), "0x02", id=2),  # block offset does not fit
        row(2, lsn(1, 1), "0x03", id=None),
        row(7, lsn(1, 2), "0x04", id=4),
        row(2, lsn(1, 3), "0x05", id=5),
    ]
    out = mapped(spark, rows)
    assert [e["order_id"] for e in events(out.valid)] == [5]
    reasons = {json.loads(r["raw_value"]).get("id"): r["reason"] for r in out.rejects.collect()}
    assert reasons == {
        1: "lsn_out_of_range",
        2: "lsn_out_of_range",
        None: "null_order_id",
        4: "invalid_op",
    }


def test_column_drift_fails_loudly(spark: SparkSession) -> None:
    extra = StructType([*SOURCE_SCHEMA.fields, StructField("note", StringType())])
    with pytest.raises(RuntimeError, match="drifted"):
        map_changes(frame(spark, [row(2, lsn(1, 1), "0x01")], extra), 0, INGESTED, -1)
    short = StructType([f for f in SOURCE_SCHEMA.fields if f.name != "amount"])
    with pytest.raises(RuntimeError, match="amount"):
        map_changes(frame(spark, [row(2, lsn(1, 1), "0x01")], short), 0, INGESTED, -1)


def test_merger_reads_the_mapped_log(spark: SparkSession, tmp_path: Path) -> None:
    lake = LakeSettings(lake_root=tmp_path.as_uri())
    snapshot = [row(0, lsn(5, 0x100), None, id=i, status="pending") for i in (1, 2, 3)]
    append_log(mapped(spark, snapshot, base=-1, batch=-1).valid, lake)
    first = run_merge(spark, lake)
    assert (first.rows_inserted, first.rows_updated, first.rows_deleted) == (3, 0, 0)

    tx1, tx2, tx3 = lsn(5, 0x200), lsn(5, 0x300), lsn(5, 0x400)
    changes = [
        row(4, tx1, "0x01", id=1, status="confirmed"),
        row(3, tx1, "0x01", id=1, status="pending"),
        row(4, tx1, "0x02", _command_id=2, id=1, status="shipped"),
        row(3, tx1, "0x02", _command_id=2, id=1, status="confirmed"),
        row(1, tx2, "0x01", id=2),
        row(2, tx3, "0x01", id=4, status="pending"),
    ]
    append_log(mapped(spark, changes, base=2, batch=0).valid, lake)

    # Rows 1 and 2 are in the table now, so the changes update 1 (twice in one transaction: the
    # last update wins), delete 2 and insert 4.
    result = run_merge(spark, lake)
    assert (result.events_read, result.distinct_orders) == (4, 3)
    assert (result.rows_inserted, result.rows_updated, result.rows_deleted) == (1, 1, 1)
    current = spark.read.format("delta").load(lake.current_orders).collect()
    statuses = {r["order_id"]: r["status"] for r in current}
    assert statuses == {1: "shipped", 3: "pending", 4: "pending"}

    again = run_merge(spark, lake)
    assert (again.rows_inserted, again.rows_updated, again.rows_deleted) == (0, 0, 0)
