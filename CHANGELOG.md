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
