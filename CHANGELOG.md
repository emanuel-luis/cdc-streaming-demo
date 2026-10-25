# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Docker Compose stack: Postgres 18 with logical replication, Kafka (KRaft), Debezium Connect,
  S3-compatible object storage, idempotent init containers for the connector and the bucket.
- `cdc-demo mutate`: seeded workload of inserts, status transitions, amount changes and deletes,
  with a JSON manifest per run.
- `cdc-demo consume`: Spark Structured Streaming from Kafka into an append-only Delta CDC log,
  with a rejects table and per-batch run metadata, written idempotently.
- `cdc-demo verify` and `make demo`: end-to-end assertions against the manifest and Postgres.
- CI (lint, unit, Spark tests, compose smoke), security workflow (pip-audit, zizmor) and
  Dependabot.
- Schema Registry (Confluent 8.3.2) and a Connect image with Confluent's Protobuf converter;
  Debezium now writes Protobuf values and keys.
- `src/cdc_demo/proto/`: the checked-in value schema (`orders_value.proto`) and its compiled
  descriptor set; `make proto` regenerates it (`grpcio-tools` and `protobuf` in the dev group).
- `cdc-demo check-schema`: compatibility check against the registry plus a text comparison;
  `make demo` runs it before the mutations.
- `spark-protobuf` package for the consumer and the Ivy warmup.

### Changed

- The consumer decodes Confluent-framed Protobuf with `from_protobuf` instead of parsing JSON.
  The CDC log, rejects and run metadata tables keep their schemas.
- Reject reasons: `unparseable_json` is replaced by `invalid_wire_header` and
  `unparseable_protobuf`; `raw_value` in the rejects table is now the base64 of the value bytes.
- Kafka UI is configured with the registry; the stack's memory budget includes Schema Registry.

### Removed

- The Debezium JSON envelope schema (`ENVELOPE_SCHEMA`) in the consumer.
