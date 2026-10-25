# Decisions

Low-impact decisions taken while building the project, with the reason. Newest last.

| # | Date | Decision | Choice | Why |
| --- | --- | --- | --- | --- |
| 1 | 2026-10-25 | Add a second CDC source with `mssql-cdc-pyspark` (SQL Server CDC read by Spark, no Debezium)? | Yes, as the last phase, "two ways to do CDC" | Shows log-based CDC through Kafka Connect and a direct Spark source side by side; it waits until the Debezium path works |
| 2 | 2026-10-25 | Protobuf from day one, or JSON first? | JSON first, Protobuf in its own phase | The first PR proves the pipeline; serialization becomes a contained change with its own contract test |
| 3 | 2026-10-25 | Tooling where the original notes and the engineering baseline differ | Baseline: uv + `pyproject.toml` only (no `requirements/*.txt`), ruff line-length 100 with `BLE`, mypy strict on `src`, Python 3.12, SHA-pinned actions | One set of rules across the portfolio |
| 4 | 2026-10-25 | Package layout | One `src/cdc_demo` package with a `cdc-demo` Typer CLI, `uv_build` backend | One install, one entry point for every container command; no work at import time |
| 5 | 2026-10-25 | Name of the component that writes to Postgres | `mutator` | "Producer" would be confused with Kafka producers, which are the sibling project's subject |
| 6 | 2026-10-25 | Spark runtime | Single container in local mode, built from `python:3.12-slim` + JRE + the `pyspark` wheel, jars resolved by Ivy at image build time | Bitnami images are no longer a free option and a master/worker pair doubles memory for no gain at demo scale |
| 7 | 2026-10-25 | Event identity in the raw log | `event_id = <topic>-<partition>-<offset>` | Deterministic: a replay produces the same ids; a random UUID would not |
| 8 | 2026-10-25 | Exactly-once into Delta with rejects and run metadata | `foreachBatch` writing with `txnAppId` + `txnVersion=batch_id` into the CDC log, the rejects table and the run log | A retried batch is skipped by Delta; rejects and per-batch counts come for free in the same step |
| 9 | 2026-10-25 | Before images | `REPLICA IDENTITY FULL` on `orders` | With the default identity Postgres sends only the key in the before image, so status transitions could not be read from updates |
| 10 | 2026-10-25 | Connector registration | Init container doing `PUT /connectors/orders-connector/config`; DB password read through Kafka's `EnvVarConfigProvider` | PUT is create-or-update, so `docker compose up` can run it every time; no password stored in the connector config |
| 11 | 2026-10-25 | Ports and naming | Compose project `cdc-streaming-demo`, no `container_name`, every host port in `.env` with the usual default, bound to 127.0.0.1 | The sibling `kafka-streaming-lakehouse` also runs Kafka and MinIO; both must coexist on one machine |
| 12 | 2026-10-25 | Ordering key for "latest event per order" (Phase 3) | `(source_lsn, kafka_offset)` | `source_ts_ms` has millisecond resolution and ties when an order changes twice in the same millisecond |
| 13 | 2026-10-25 | Merger watermark (Phase 3) | Last Kafka offset per partition in a Delta control table, moved only after the MERGE commits; MERGE updates only when the event is newer than the stored row | Consumer batches commit in offset order, so an offset watermark never skips rows; the newer-than guard makes a re-run after a crash harmless |
| 14 | 2026-10-25 | How phases ship | One PR per phase, each stacked on the previous branch | The PRs stay reviewable while earlier ones wait for merge |
| 15 | 2026-10-25 | Spark tests | Marker `spark`, deselected by default; run in CI on Linux and locally in WSL or containers | Spark does not run on Windows Python on the owner's machine |
| 16 | 2026-10-25 | Mutator determinism | Sync psycopg, one transaction per mutation, rows picked in Python with a seeded RNG | With a seed and the seeded initial data, two runs make the same changes |
| 17 | 2026-10-25 | Local object storage (the original notes say MinIO) | RustFS 1.0.3 behind generic `S3_*` settings; SeaweedFS 4.50 as fallback | The MinIO community repo was archived in April 2026 and no `minio/minio` or `minio/mc` image resolves on Docker Hub or quay.io any more; the code only speaks S3 (`s3a://`), so the server is swappable |
| 18 | 2026-10-25 | Versions | Spark 4.2.0 + delta-spark 4.4.1, Confluent Platform 8.3.2 (KRaft only), Debezium 3.7.0.Final from quay.io, Postgres 18.6 | Current stable lines on 2026-10-25; Delta 4.4.1 supports Spark 4.0.1 to 4.2.0 and Debezium 3.7 supports Postgres 14 to 18 |
| 19 | 2026-10-25 | Debezium database user | Dedicated `cdc_reader` role with `REPLICATION` and `SELECT` on `orders`; the publication is created by the owner in initdb (`publication.autocreate.mode=disabled`) | Least privilege: the capture user cannot write to the source |
| 20 | 2026-10-25 | Topic creation | Broker auto-create off; the connector creates `cdc.public.orders` through `topic.creation.default.*` | A consumer subscribing early cannot create the topic with the wrong settings |
