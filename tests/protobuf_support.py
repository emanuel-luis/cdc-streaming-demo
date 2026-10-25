"""Build Confluent-framed Protobuf Kafka values in tests, from the committed descriptor set."""

import json
from pathlib import Path
from typing import Any

from google.protobuf import descriptor_pb2, descriptor_pool, json_format, message_factory
from google.protobuf.message import Message

from cdc_demo.proto import VALUE_MESSAGE, value_descriptor_set

FIXTURES = Path(__file__).parent / "fixtures"
SCHEMA_ID = 2  # the id in the registry fixture and in the golden record


def encode_header(schema_id: int) -> bytes:
    """Header of the first message in the schema: magic 0, big-endian id, message index [0]."""
    return b"\x00" + schema_id.to_bytes(4, "big") + b"\x00"


def envelope_class() -> type[Message]:
    fds = descriptor_pb2.FileDescriptorSet.FromString(value_descriptor_set())
    pool = descriptor_pool.DescriptorPool()
    for file in fds.file:
        pool.Add(file)
    return message_factory.GetMessageClass(pool.FindMessageTypeByName(VALUE_MESSAGE))


def payload(event: dict[str, Any]) -> bytes:
    """Serialize a Debezium-shaped dict (the JSON fixtures) as an Envelope."""
    return json_format.ParseDict(event, envelope_class()()).SerializeToString()


def kafka_value(event: dict[str, Any], schema_id: int = SCHEMA_ID) -> bytes:
    return encode_header(schema_id) + payload(event)


def fixture_event(name: str) -> dict[str, Any]:
    event: dict[str, Any] = json.loads((FIXTURES / f"{name}.json").read_text(encoding="utf-8"))
    return event


def golden_record() -> bytes:
    return bytes.fromhex((FIXTURES / "snapshot-record.hex").read_text(encoding="ascii").strip())
