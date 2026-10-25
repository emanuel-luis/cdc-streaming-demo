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
- `cdc-demo merge`: batch merger from the CDC log into `current/orders` (one row per live
  order). It reads the log at a pinned Delta version, deduplicates by `(source_lsn,
  kafka_offset)`, applies a MERGE with a newer-than guard and moves an offset watermark only
  after the MERGE commits. Control tables `_meta/merge_watermark` and `_meta/merge_runs`.
- `cdc-demo verify-current`: compares the current table with Postgres row by row.
- `make merge`; `make demo` runs the merge twice (the second run must change nothing) and
  verifies the current table against Postgres.
- `cdc-demo build-marts`: `dbt build` over the Delta tables with dbt-duckdb (DuckDB's `delta`
  extension reads them in place from S3) into a staging DuckDB file that is renamed over
  `analytics.duckdb` only when every model and test passed.
- `dbt/`: staging views and four marts (`dim_orders_current`, `fct_order_status_changes`,
  `fct_order_activity_hourly`, `fct_pipeline_runs`) with enforced contracts, generic and
  singular tests, including a recomputation of the current table from the CDC log.
- `cdc-demo verify-marts`: checks the marts against the mutator manifest.
- `dashboard/app.py`: read-only Streamlit dashboard (`make dashboard`).
- `analytics` image target, `analytics` and `dashboard` compose services, `warehouse` volume;
  `make marts`; `make demo` now has 10 steps and ends with `build-marts` and `verify-marts`.
- `dbt parse` in `make lint` and in the CI lint job; Dependabot ignores for the dbt and DuckDB
  minor versions and for the protobuf and grpcio-tools lines.

### Changed

- The consumer decodes Confluent-framed Protobuf with `from_protobuf` instead of parsing JSON.
  The CDC log, rejects and run metadata tables keep their schemas.
- Reject reasons: `unparseable_json` is replaced by `invalid_wire_header` and
  `unparseable_protobuf`; `raw_value` in the rejects table is now the base64 of the value bytes.
- The consumer creates the CDC log, the rejects table and `ingest_runs` at startup, so the
  rejects table exists before the first reject. The create helper moved to
  `cdc_demo.spark.tables`.
- Dev group: `grpcio-tools>=1.80,<1.83` and `protobuf>=6.33.5,<7` (dbt-common needs
  protobuf below 7).
- Kafka UI is configured with the registry; the stack's memory budget includes Schema Registry.

### Removed

- The Debezium JSON envelope schema (`ENVELOPE_SCHEMA`) in the consumer.
