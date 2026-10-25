"""The consumer's Protobuf contract: the registered value schema and its compiled descriptor set."""

from importlib.resources import files

VALUE_PROTO = "orders_value.proto"
VALUE_DESCRIPTOR = "orders_value.desc"
# Fully qualified name of the top-level message the Confluent serializer writes (index [0]).
VALUE_MESSAGE = "cdc.public.orders.Envelope"


def value_proto_text() -> str:
    return (files(__package__) / VALUE_PROTO).read_text(encoding="utf-8")


def value_descriptor_set() -> bytes:
    return (files(__package__) / VALUE_DESCRIPTOR).read_bytes()
