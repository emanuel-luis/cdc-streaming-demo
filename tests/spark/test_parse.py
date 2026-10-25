import base64
import copy
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import pytest
from protobuf_support import (
    encode_header,
    envelope_class,
    fixture_event,
    golden_record,
    kafka_value,
)
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.protobuf.functions import from_protobuf

from cdc_demo.ingest.parse import (
    REASON_BAD_FIELD,
    REASON_BAD_HEADER,
    REASON_INVALID_OP,
    REASON_NO_AFTER,
    REASON_NO_ORDER_ID,
    REASON_UNPARSEABLE,
    parse_batch,
)
from cdc_demo.ingest.schemas import CDC_LOG_SCHEMA, REJECTS_SCHEMA
from cdc_demo.proto import VALUE_MESSAGE, value_descriptor_set

pytestmark = pytest.mark.spark

INGESTED_AT = datetime(2026, 10, 26, 3, 0, tzinfo=UTC)
TOPIC = "cdc.public.orders"


def raw_batch(spark: SparkSession, values: list[bytes | None]) -> DataFrame:
    """Rows shaped like the Kafka source: key, value, topic, partition, offset, timestamp."""
    rows = [(b"k", v, TOPIC, 0, 100 + i, INGESTED_AT) for i, v in enumerate(values)]
    return spark.createDataFrame(
        rows,
        "key binary, value binary, topic string, partition int, offset long, timestamp timestamp",
    )


def value_of(name: str, **changes: Any) -> bytes:
    """A Kafka value from a JSON fixture, with top-level fields replaced (None removes one)."""
    event = copy.deepcopy(fixture_event(name))
    for key, new in changes.items():
        if new is None:
            event.pop(key, None)
        else:
            event[key] = new
    return kafka_value(event)


def test_schemas_match_exactly(spark: SparkSession) -> None:
    p = parse_batch(raw_batch(spark, [value_of("create"), b"nope"]), 3, INGESTED_AT)
    assert [(f.name, f.dataType) for f in p.valid.schema] == [
        (f.name, f.dataType) for f in CDC_LOG_SCHEMA
    ]
    assert [(f.name, f.dataType) for f in p.rejects.schema] == [
        (f.name, f.dataType) for f in REJECTS_SCHEMA
    ]


def test_create_update_delete_snapshot(spark: SparkSession) -> None:
    values = [value_of(n) for n in ("snapshot", "create", "update", "delete")]
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


def test_absent_images_decode_as_null(spark: SparkSession) -> None:
    # A message field that is not on the wire must be null, not a struct of nulls.
    values = [value_of("delete"), value_of("snapshot")]
    decoded = raw_batch(spark, values).select(
        from_protobuf(
            F.expr("substring(value, 7)"),
            VALUE_MESSAGE,
            binaryDescriptorSet=value_descriptor_set(),
            options={"mode": "PERMISSIVE"},
        ).alias("env")
    )
    delete, snapshot = decoded.collect()
    assert delete["env"]["after"] is None and delete["env"]["before"] is not None
    assert snapshot["env"]["before"] is None and snapshot["env"]["after"] is not None


def test_golden_record_from_the_real_converter(spark: SparkSession) -> None:
    value = golden_record()
    expected = envelope_class()()
    expected.ParseFromString(value[6:])
    p = parse_batch(raw_batch(spark, [value]), 0, INGESTED_AT)
    assert p.rejects.count() == 0
    row = p.valid.first()
    assert row is not None
    assert row["op"] == "r" == expected.op  # type: ignore[attr-defined]
    after, source = expected.after, expected.source  # type: ignore[attr-defined]
    assert row["order_id"] == after.id and row["customer_id"] == after.customer_id
    assert row["status"] == after.status and row["amount"] == Decimal(after.amount)
    assert row["before_status"] is None
    assert row["source_ts_ms"] == source.ts_ms and row["source_lsn"] == source.lsn
    assert row["source_tx_id"] == source.txId


def test_partition_date_is_utc_of_source_ts(spark: SparkSession) -> None:
    # create: 1793016000130 ms = 2026-10-26T12:00:00Z
    # delete: 1793102399999 ms = 2026-10-27T11:59:59Z
    p = parse_batch(raw_batch(spark, [value_of("create"), value_of("delete")]), 0, INGESTED_AT)
    dates = {r["op"]: r["_partition_date"] for r in p.valid.collect()}
    assert dates == {"c": date(2026, 10, 26), "d": date(2026, 10, 27)}


def test_tombstone_skipped_and_counted(spark: SparkSession) -> None:
    p = parse_batch(raw_batch(spark, [None, None]), 0, INGESTED_AT)
    assert p.tombstones == 2
    assert p.valid.count() == 0 and p.rejects.count() == 0


def test_rejects_with_reasons(spark: SparkSession) -> None:
    good = value_of("create")
    values: list[bytes | None] = [
        b"\x01" + good[1:],  # 100: magic byte is not 0
        good[:5] + b"\x02" + good[6:],  # 101: message index is not [0]
        b"\x00\x00\x00",  # 102: shorter than the header
        # 103: field 2 (after) claims 255 bytes and ends there
        encode_header(2) + b"\x12\xff\x01",
        value_of("create", op="x"),  # 104
        value_of("create", after=None),  # 105
        value_of("delete", before=None),  # 106
        value_of("update"),  # 107: valid
    ]
    p = parse_batch(raw_batch(spark, values), 4, INGESTED_AT)
    reasons = {r["kafka_offset"]: r["reason"] for r in p.rejects.collect()}
    assert reasons == {
        100: REASON_BAD_HEADER,
        101: REASON_BAD_HEADER,
        102: REASON_BAD_HEADER,
        103: REASON_UNPARSEABLE,
        104: REASON_INVALID_OP,
        105: REASON_NO_AFTER,
        106: REASON_NO_ORDER_ID,
    }
    reject = p.rejects.filter("kafka_offset = 102").first()
    assert reject is not None
    assert reject["raw_value"] == base64.b64encode(b"\x00\x00\x00").decode()
    assert reject["ingest_batch_id"] == 4
    assert reject["_partition_date"] == date(2026, 10, 26)
    assert p.valid.count() == 1


def test_bad_amount_is_rejected_not_fatal(spark: SparkSession) -> None:
    event = copy.deepcopy(fixture_event("create"))
    event["after"]["amount"] = "abc"
    p = parse_batch(raw_batch(spark, [kafka_value(event)]), 0, INGESTED_AT)
    assert p.valid.count() == 0
    row = p.rejects.first()
    assert row is not None and row["reason"] == REASON_BAD_FIELD
