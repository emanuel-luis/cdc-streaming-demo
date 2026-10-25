from collections.abc import Callable
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from pyspark.sql import DataFrame, SparkSession

from cdc_demo.ingest.parse import (
    REASON_INVALID_OP,
    REASON_NO_AFTER,
    REASON_NO_ORDER_ID,
    REASON_UNPARSEABLE,
    parse_batch,
)
from cdc_demo.ingest.schemas import CDC_LOG_SCHEMA, REJECTS_SCHEMA

pytestmark = pytest.mark.spark

INGESTED_AT = datetime(2026, 10, 26, 3, 0, tzinfo=UTC)
TOPIC = "cdc.public.orders"


def raw_batch(spark: SparkSession, values: list[str | None]) -> DataFrame:
    """Rows shaped like the Kafka source: key, value, topic, partition, offset, timestamp."""
    rows = [
        (b"k", None if v is None else v.encode(), TOPIC, 0, 100 + i, INGESTED_AT)
        for i, v in enumerate(values)
    ]
    return spark.createDataFrame(
        rows,
        "key binary, value binary, topic string, partition int, offset long, timestamp timestamp",
    )


def test_schemas_match_exactly(spark: SparkSession, fixture_text: Callable[[str], str]) -> None:
    p = parse_batch(raw_batch(spark, [fixture_text("create"), "nope"]), 3, INGESTED_AT)
    assert [(f.name, f.dataType) for f in p.valid.schema] == [
        (f.name, f.dataType) for f in CDC_LOG_SCHEMA
    ]
    assert [(f.name, f.dataType) for f in p.rejects.schema] == [
        (f.name, f.dataType) for f in REJECTS_SCHEMA
    ]


def test_create_update_delete_snapshot(
    spark: SparkSession, fixture_text: Callable[[str], str]
) -> None:
    values = [fixture_text(n) for n in ("snapshot", "create", "update", "delete")]
    p = parse_batch(raw_batch(spark, values), 9, INGESTED_AT)
    assert p.tombstones == 0 and p.rejects.count() == 0
    rows = {r["op"]: r for r in p.valid.collect()}
    assert set(rows) == {"r", "c", "u", "d"}

    c = rows["c"]
    assert c["event_id"] == f"{TOPIC}-0-101"
    assert c["order_id"] == 501 and c["customer_id"] == 42 and c["status"] == "pending"
    assert c["amount"] == Decimal("123.45")
    assert c["before_status"] is None and c["before_amount"] is None
    assert c["created_at"].astimezone(UTC) == datetime(2026, 10, 25, 12, 0, 0, 123456, tzinfo=UTC)
    assert c["source_lsn"] == 101 and c["source_tx_id"] == 812
    assert c["ingest_batch_id"] == 9

    u = rows["u"]
    assert u["before_status"] == "pending" and u["status"] == "confirmed"
    assert u["updated_at"].astimezone(UTC) == datetime(2026, 10, 25, 12, 0, 5, tzinfo=UTC)

    d = rows["d"]
    assert d["order_id"] == 501 and d["status"] is None and d["amount"] is None
    assert d["before_amount"] == Decimal("99.90") and d["before_status"] == "cancelled"

    assert rows["r"]["order_id"] == 7


def test_partition_date_is_utc_of_source_ts(
    spark: SparkSession, fixture_text: Callable[[str], str]
) -> None:
    # create: 1793016000130 ms = 2026-10-26T12:00:00Z; delete: 1793102399999 ms = 2026-10-27T11:59:59Z
    p = parse_batch(
        raw_batch(spark, [fixture_text("create"), fixture_text("delete")]), 0, INGESTED_AT
    )
    dates = {r["op"]: r["_partition_date"] for r in p.valid.collect()}
    assert dates == {"c": date(2026, 10, 26), "d": date(2026, 10, 27)}


def test_tombstone_skipped_and_counted(spark: SparkSession) -> None:
    p = parse_batch(raw_batch(spark, [None, None]), 0, INGESTED_AT)
    assert p.tombstones == 2
    assert p.valid.count() == 0 and p.rejects.count() == 0


def test_rejects_with_reasons(spark: SparkSession, fixture_text: Callable[[str], str]) -> None:
    no_after = fixture_text("create").replace('"after": {', '"after_x": {')
    unknown_op = fixture_text("create").replace('"op": "c"', '"op": "x"')
    no_id_delete = '{"before": null, "after": null, "source": {"ts_ms": 1}, "op": "d"}'
    values = ["{not json", unknown_op, no_after, no_id_delete, fixture_text("update")]
    p = parse_batch(raw_batch(spark, values), 4, INGESTED_AT)
    reasons = {r["kafka_offset"]: r["reason"] for r in p.rejects.collect()}
    assert reasons == {
        100: REASON_UNPARSEABLE,
        101: REASON_INVALID_OP,
        102: REASON_NO_AFTER,
        103: REASON_NO_ORDER_ID,
    }
    reject = p.rejects.filter("kafka_offset = 100").first()
    assert reject is not None
    assert reject["raw_value"] == "{not json"
    assert reject["ingest_batch_id"] == 4
    assert reject["_partition_date"] == date(2026, 10, 26)
    assert p.valid.count() == 1


def test_bad_amount_is_rejected_not_fatal(
    spark: SparkSession, fixture_text: Callable[[str], str]
) -> None:
    bad = fixture_text("create").replace('"123.45"', '"abc"')
    p = parse_batch(raw_batch(spark, [bad]), 0, INGESTED_AT)
    assert p.valid.count() == 0
    row = p.rejects.first()
    assert row is not None and row["reason"] == "invalid_field"
