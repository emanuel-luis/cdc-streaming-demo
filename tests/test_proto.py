"""The checked-in Protobuf contract: descriptor in step with the .proto, golden record decodes."""

import importlib.util
import sys
from pathlib import Path

import pytest
from google.protobuf import descriptor_pb2
from protobuf_support import SCHEMA_ID, encode_header, envelope_class, golden_record

from cdc_demo.proto import VALUE_MESSAGE, value_descriptor_set, value_proto_text

ROOT = Path(__file__).parent.parent


def test_descriptor_set_matches_the_proto(tmp_path: Path) -> None:
    pytest.importorskip("grpc_tools")
    spec = importlib.util.spec_from_file_location(
        "compile_proto", ROOT / "scripts" / "compile_proto.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["compile_proto"] = module
    spec.loader.exec_module(module)

    out = tmp_path / "orders_value.desc"
    module.compile_descriptor(ROOT / "src" / "cdc_demo" / "proto", out)
    fresh = descriptor_pb2.FileDescriptorSet.FromString(out.read_bytes())
    committed = descriptor_pb2.FileDescriptorSet.FromString(value_descriptor_set())
    assert fresh == committed, "orders_value.desc is stale: run `make proto`"


def test_value_message_is_in_the_descriptor_set() -> None:
    assert envelope_class().DESCRIPTOR.full_name == VALUE_MESSAGE
    assert "message Envelope" in value_proto_text()


def test_header_helper_matches_the_golden_record() -> None:
    assert encode_header(SCHEMA_ID) == golden_record()[:6]
    assert encode_header(7) == b"\x00\x00\x00\x00\x07\x00"


def test_golden_record_is_a_snapshot_event() -> None:
    event = envelope_class()()
    event.ParseFromString(golden_record()[6:])
    assert event.op == "r"  # type: ignore[attr-defined]
    assert not event.HasField("before")
    assert event.after.id > 0  # type: ignore[attr-defined]
