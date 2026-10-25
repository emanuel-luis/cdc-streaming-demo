# Implementation plan

The plan this repository is built from. It refreshes the original planning notes against the
engineering baseline (uv, ruff 100, mypy strict, SHA-pinned actions) and current versions.
Decisions and their reasons live in [DECISIONS.md](DECISIONS.md).

## Data flow and layers

| Layer | Where | What | Written by |
| --- | --- | --- | --- |
| Source | Postgres `public.orders` | OLTP table, mutated by the mutator | `cdc-demo mutate` |
| Transport | Kafka topic `cdc.public.orders` | Debezium change events, keyed by `id` | Debezium Connect |
| Raw (landing) | Delta `s3a://cdc-demo/raw/orders_cdc_log` | Append-only CDC log, one row per change event, never updated | `cdc-demo consume` |
| Raw rejects | Delta `s3a://cdc-demo/raw/_rejects/orders_cdc_log` | Events that broke the contract, with the reason and Kafka coordinates | `cdc-demo consume` |
| Run metadata | Delta `s3a://cdc-demo/_meta/ingest_runs` | One row per micro-batch: run id, batch id, counts, offsets, UTC times | `cdc-demo consume` |
| Processed (current) | Delta `s3a://cdc-demo/current/orders` | One row per live order: latest state | `cdc-demo merge` |
| Merger control | Delta `s3a://cdc-demo/_meta/merge_watermark`, `_meta/merge_runs` | Watermark per partition; one row per merge run | `cdc-demo merge` |
| Serving | DuckDB file built by dbt | `stg_orders`, `dim_orders_current`, `fct_order_status_changes` (Phase 5) | `dbt build` |
| Presentation | Streamlit | Status mix, revenue, recent transitions (Phase 5) | dashboard |

Checkpoints live under `s3a://cdc-demo/_checkpoints/<query>`.

## Repository layout

```
compose.yaml                 # whole stack, project name cdc-streaming-demo
.env.example                 # every port, credential and tunable, commented
Makefile                     # help, install, lint, proto, test(-fast, -spark), up, down, demo
docker/app.Dockerfile        # targets: mutator (slim) and spark (JRE + pyspark + baked jars)
docker/connect.Dockerfile    # Debezium Connect + Confluent Protobuf converter (Phase 2)
postgres/initdb/             # schema, seed (seeded random), CDC role, publication
debezium/orders-connector.json
scripts/                     # register-connector.sh, demo.sh, wait helpers, compile_proto.py
src/cdc_demo/                # one package, uv_build backend, py.typed
  cli.py                     # Typer app: mutate, consume, verify, check-schema, merge, verify-current
  settings.py                # pydantic-settings, one class per component, env prefixes
  logging.py                 # structured log lines (run_id, batch_id, counts)
  mutator/                   # status machine, op selection, SQL, manifest
  spark/session.py           # SparkSession builder (Delta, Kafka, s3a, UTC)
  contract.py                # check-schema: checked-in .proto vs Schema Registry (stdlib only)
  proto/                     # orders_value.proto (registry text) + orders_value.desc (compiled)
  ingest/                    # table schemas, parse + validate, foreachBatch writer
  merge/                     # current table + control table schemas, run_merge (Phase 3)
  verify.py                  # end-to-end assertions used by make demo
  verify_current.py          # current table vs Postgres, row by row
tests/                       # unit (no JVM), spark-marked, integration-marked
docs/                        # PLAN.md, DECISIONS.md
.github/workflows/           # ci.yml, security.yml; dependabot.yml
```

## Contracts

`public.orders` (Postgres): `id BIGSERIAL PK, customer_id INT, status VARCHAR(20) CHECK IN
(pending, confirmed, shipped, delivered, cancelled), amount NUMERIC(12,2) CHECK > 0,
created_at TIMESTAMPTZ, updated_at TIMESTAMPTZ` (trigger keeps `updated_at`). `REPLICA IDENTITY
FULL` so updates and deletes carry the full before image. Seed: 500 rows generated with
`setseed()` so every fresh stack starts from the same data.

Debezium (Phase 1, JSON without schemas): envelope `{before, after, source, op, ts_ms}`;
`amount` as string (`decimal.handling.mode=string`); `TIMESTAMPTZ` as ISO-8601 strings
(`io.debezium.time.ZonedTimestamp`); deletes are followed by a tombstone (null value), which the
consumer skips and counts.

Debezium (Phase 2, Protobuf): keys and values go through Confluent's `ProtobufConverter`, which
registers `cdc.public.orders-key` and `cdc.public.orders-value` in Schema Registry. The value is
`cdc.public.orders.Envelope` with the same fields as above (`amount` and the timestamps stay
strings). Wire format of a Kafka value: byte 0 is the magic byte 0, bytes 1-4 the schema id
(big-endian), byte 5 the message index `[0]` as one 0 byte, then the Protobuf payload. The
consumer ignores the schema id and decodes with `from_protobuf` and the descriptor set compiled
from the checked-in `src/cdc_demo/proto/orders_value.proto`. Proto3 scalars at their zero value
decode as null (decision 26). Tombstones are unchanged. `cdc-demo check-schema` fails when the
registered value schema is incompatible with, or differs from, the checked-in one.

Raw CDC log (Delta, partitioned by `_partition_date`):

| Column | Type | Notes |
| --- | --- | --- |
| event_id | STRING | `<topic>-<partition>-<offset>`, deterministic |
| op | STRING | c, u, d, r |
| order_id | BIGINT | `coalesce(after.id, before.id)`, never null |
| customer_id, status, amount, created_at, updated_at | INT, STRING, DECIMAL(12,2), TIMESTAMP, TIMESTAMP | after image, null on delete |
| before_status, before_amount | STRING, DECIMAL(12,2) | before image, null on create and snapshot |
| source_ts_ms, source_lsn, source_tx_id | BIGINT | Debezium `source` block |
| kafka_topic, kafka_partition, kafka_offset, kafka_timestamp | STRING, INT, BIGINT, TIMESTAMP | coordinates |
| ingest_batch_id | BIGINT | Structured Streaming batch id |
| ingested_at | TIMESTAMP | UTC |
| _partition_date | DATE | from `source_ts_ms`, UTC |

Validation: a row is rejected, with one of these reasons, when

- `invalid_wire_header`: the value is shorter than 6 bytes, byte 0 is not 0, or byte 5 is not 0;
- `unparseable_protobuf`: the payload does not decode as an `Envelope`;
- `invalid_op`: `op` is not one of c/u/d/r;
- `missing_after_image`: a c/u/r event has no after image;
- `null_order_id`: `coalesce(after.id, before.id)` is null;
- `invalid_field`: `amount`, `created_at` or `updated_at` does not convert.

Rejects keep the coordinates and the reason; `raw_value` is the base64 of the Kafka value
bytes (decision 24).

Current table (Delta `current/orders`, unpartitioned, one row per live order; decision 28):

| Column | Type | Notes |
| --- | --- | --- |
| order_id | BIGINT NOT NULL | merge key |
| customer_id, status, amount, created_at, updated_at | INT, STRING, DECIMAL(12,2), TIMESTAMP, TIMESTAMP | as in Postgres |
| _source_lsn | BIGINT | LSN of the event that produced the row |
| _kafka_partition, _kafka_offset | INT, BIGINT | coordinates of that event |
| _event_id, _op | STRING | event id and op (c, u, r) of that event |
| _merged_at, _merge_run_id | TIMESTAMP, STRING | when and by which run the row was last written |

Watermark (`_meta/merge_watermark`): `kafka_topic STRING, kafka_partition INT, last_offset
BIGINT, updated_at TIMESTAMP, run_id STRING`, one row per topic and partition.

Merge runs (`_meta/merge_runs`, one row per run, appended last): `run_id, started_at,
finished_at, cdc_log_version, events_read, distinct_orders, rows_inserted, rows_updated,
rows_deleted, watermark_before, watermark_after`. The watermarks are JSON strings
`{"<topic>:<partition>": offset}`. `cdc_log_version` is null when the CDC log does not exist
yet; the row counts come from the MERGE operation metrics in the current table's history and
are 0 when no MERGE ran.

Merge run, in order:

1. Create the three tables from explicit schemas if missing.
2. Pin the latest CDC log version and read it once with `versionAsOf`.
3. Keep events with `kafka_offset` above the watermark of their topic and partition (broadcast
   join; no watermark row means read everything).
4. Keep one event per `order_id`, latest by `(source_lsn, kafka_offset)`.
5. MERGE on `order_id`. Matched and the event is newer
   (`s.source_lsn > t._source_lsn` or equal LSN and a higher offset): delete on `d`, update on
   c/u/r. Not matched: insert on c/u/r, nothing on `d`.
6. After the MERGE commits, upsert the watermark with the highest offset per partition of the
   same read.
7. Append the run row and log one `merge_finished` JSON line.

## Runtime contract (Phase 1-2)

The Python package and the container stack meet here; change both sides together.

Pinned versions (checked 2026-10-25):

| Piece | Pin |
| --- | --- |
| Postgres | `postgres:18.6-alpine` (PG 18 moved PGDATA: mount the volume at `/var/lib/postgresql`) |
| Kafka | `confluentinc/cp-kafka:8.3.2`, KRaft single node (broker + controller) |
| Schema Registry | `confluentinc/cp-schema-registry:8.3.2` (compatibility `backward`) |
| Connect image | `docker/connect.Dockerfile`: Debezium 3.7.0.Final + Confluent Protobuf converter 8.3.2 |
| Debezium | `quay.io/debezium/connect:3.7.0.Final` (Docker Hub `debezium/connect` stopped at 3.0) |
| Object storage | `rustfs/rustfs:1.0.3` (S3-compatible, Apache-2.0); fallback `chrislusf/seaweedfs:4.50` |
| Init containers | `curlimages/curl:8.23.0` (connector PUT, bucket create with `--aws-sigv4`) |
| Kafka UI (profile `ui`) | `ghcr.io/kafbat/kafka-ui:v1.5.0` |
| App base | `python:3.12.15-slim-bookworm` + JRE from `eclipse-temurin:17.0.20.1_1-jre-noble` + `ghcr.io/astral-sh/uv:0.13.5` |
| Spark | `pyspark==4.2.0` (Scala 2.13), `delta-spark==4.4.1` |
| Spark packages | `io.delta:delta-spark_2.13:4.4.1`, `org.apache.spark:spark-sql-kafka-0-10_2.13:<pyspark version>`, `org.apache.hadoop:hadoop-aws:<hadoop version bundled in pyspark/jars>`, `org.apache.spark:spark-protobuf_2.13:<pyspark version>` |

Environment (names are the contract; defaults in `.env.example`):

| Variable | Default | Used by |
| --- | --- | --- |
| `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD` | `localhost`, `5432`, `cdc_demo`, `cdc_owner`, `cdc_owner_pw` | mutator, verify, postgres image |
| `CDC_DB_USER`, `CDC_DB_PASSWORD` | `cdc_reader`, `cdc_reader_pw` | Debezium (replication role, created by initdb) |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` (`kafka:29092` inside compose) | consumer |
| `CDC_TOPIC` | `cdc.public.orders` | consumer, check-schema (subject `<CDC_TOPIC>-value`) |
| `SCHEMA_REGISTRY_URL` | `http://localhost:8081` (`http://schema-registry:8081` inside compose) | check-schema |
| `S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_REGION` | `http://localhost:9000`, `cdc_demo_admin`, `cdc_demo_secret`, `us-east-1` | consumer, verify, s3 image |
| `LAKE_ROOT` | `s3a://cdc-demo` | consumer, verify (a `file://` path in tests) |
| `CONSUMER_TRIGGER_INTERVAL`, `CONSUMER_MAX_OFFSETS_PER_TRIGGER`, `CONSUMER_APP_ID` | `10 seconds`, `10000`, `orders-cdc-consumer-v1` | consumer |
| `SPARK_MASTER`, `SPARK_DRIVER_MEMORY`, `SPARK_IVY_DIR` | `local[2]`, `1g`, unset (`/opt/spark-ivy` in the image) | Spark jobs |
| `MUTATOR_RATE`, `MUTATOR_SEED`, `MUTATOR_INSERT_WEIGHT`, `MUTATOR_UPDATE_WEIGHT`, `MUTATOR_DELETE_WEIGHT`, `MUTATOR_CANCEL_PROBABILITY` | `20`, unset, `0.5`, `0.4`, `0.1`, `0.1` | mutator |
| Host ports: `POSTGRES_PORT`, `KAFKA_PORT`, `CONNECT_PORT`, `SCHEMA_REGISTRY_PORT`, `S3_API_PORT`, `S3_CONSOLE_PORT`, `SPARK_UI_PORT`, `KAFKA_UI_PORT` | 5432, 9092, 8083, 8081, 9000, 9001, 4040, 8090 | compose, bound to 127.0.0.1 |

CLI (`cdc-demo`, Typer):

- `cdc-demo mutate [--count N | --duration S] [--rate R] [--seed S] [--manifest PATH]`: one
  transaction per mutation; writes a JSON manifest `{run_id, started_at, finished_at, seed,
  requested, counts: {c, u, d}, skipped}` (UTC ISO-8601) and logs one line per mutation.
- `cdc-demo consume [--available-now]`: the streaming job; `--available-now` drains the topic
  and stops (tests, backfills).
- `cdc-demo check-schema`: GETs the latest `<CDC_TOPIC>-value` version from the registry, asks
  its compatibility API whether the checked-in `.proto` is compatible, and compares both texts
  (comments, trailing whitespace and blank lines dropped). Prints a report; exits 0 only when
  compatible and identical, 1 otherwise or on an HTTP error. Standard library only (the mutator
  image runs it).
- `cdc-demo verify --manifest PATH [--snapshot-rows 500] [--timeout 300]`: polls the Delta
  tables and Postgres until the expectations hold or the timeout passes; prints expected vs
  actual and exits 1 on mismatch. Assumes fresh volumes (one mutator run).
- `cdc-demo merge`: one batch merge run (see above); prints events read, orders, rows inserted,
  updated and deleted, and the watermark before and after. Safe to re-run.
- `cdc-demo verify-current [--timeout S]`: compares `current/orders` with Postgres on
  order_id, customer_id, status, amount, created_at and updated_at. Prints `current_rows,
  postgres_rows, missing_in_current, extra_in_current, mismatched_rows, duplicate_keys` and up
  to 10 example ids per problem; exits 0 only when everything matches. `--timeout` (default 0) re-checks until
  the tables match or the time passes.

Containers:

- `docker/app.Dockerfile` target `mutator` (no JVM) and target `spark` (JRE, `--extra spark`,
  Ivy cache baked at build time into `/opt/spark-ivy`, so runs need no network). Both run as a
  non-root user, entrypoint `cdc-demo`.
- Compose services: `postgres`, `kafka`, `schema-registry`, `connect`, `connector-init`
  (one-shot), `s3`, `s3-init` (one-shot), `consumer` (long-running, `restart: on-failure`),
  `mutator` and `spark-job` (profile `tools`, used through `docker compose run --rm`; `spark-job`
  runs the image `consumer` builds, so one build exports it),
  `kafka-ui` (profile `ui`). Shared named volume `demo_runs` mounted at `/runs` for manifests.
- Broker auto topic creation is off; the connector creates its topic through
  `topic.creation.default.*` (1 partition, replication 1).
- `make demo`: `down -v`, `--profile tools build` (`up` skips that profile and `run` never
  rebuilds a stale image), `up -d --wait`, wait until the topic holds the 500 snapshot events,
  `run mutator check-schema`, `run mutator mutate --count 200 --seed 42`,
  `run spark-job verify`, `run spark-job merge` twice (the second run must report 0 inserted,
  updated and deleted), `run spark-job verify-current`. The stack stays up for exploration; `make down` removes it with its
  volumes.

## Idempotency rules

- Connector registration is `PUT /connectors/orders-connector/config` (create or update).
- The consumer writes each micro-batch through `foreachBatch` with Delta's `txnAppId` and
  `txnVersion=batch_id`: a retried batch is skipped by Delta instead of written twice.
- `event_id` derives from Kafka coordinates, so a replay produces the same ids.
- Phase 3: the merger keeps one event per `order_id` ordered by `(source_lsn, kafka_offset)`,
  updates only when the incoming event is newer than the stored one, and moves its watermark
  (last Kafka offset per partition, in a Delta control table) only after the MERGE commits.

## Phases and pull requests

Each phase is one PR, stacked on the previous branch.

1. **Minimum Shippable** (`feat/minimum-shippable`): tooling (pyproject, uv.lock, ruff, mypy,
   pytest, Makefile, CI, security, Dependabot), compose stack (Postgres, Kafka KRaft, Debezium
   Connect, connector init, MinIO, bucket init, consumer, mutator and verify on demand), the
   mutator with manifest, the consumer landing the raw CDC log with rejects and run metadata,
   `cdc-demo verify`, `make demo`, README walkthrough, CLAUDE.md, CHANGELOG, LICENSE.
   Done when `make demo` on fresh volumes asserts: 500 snapshot rows, c/u/d counts equal to the
   mutator manifest, no rejects, unique `event_id`, Postgres row count equal to
   500 + inserts - deletes.
2. **Protobuf + Schema Registry** (`feat/protobuf`): Schema Registry service, custom Connect
   image with Confluent's Protobuf converter, connector switched to Protobuf, consumer decodes
   the Confluent wire format with Spark's `from_protobuf` and a descriptor built from the
   checked-in `.proto`, a contract test that compares the registered schema with the checked-in
   one, same CDC log contract as Phase 1, and `make proto` to recompile the descriptor.
3. **CDC-to-current merger** (`feat/merger`): `cdc-demo merge` (batch, re-runnable), the current
   table, the watermark control table, unit tests for dedup and MERGE on temp Delta tables, and
   `make demo` extended to assert the current table equals Postgres row by row.
4. **dbt + dashboard** (`feat/dbt-dashboard`): dbt-duckdb project reading the Delta tables
   through DuckDB's delta extension, staging and marts with tests, Streamlit dashboard, demo
   extended with `dbt build`.
5. **Two ways to do CDC** (`feat/mssql-cdc-comparison`): SQL Server CDC read directly by Spark
   with the `mssql-cdc-pyspark` library into the same CDC log contract, and a comparison section
   (operational cost, latency, ordering, deletes, schema changes).

## Verification per phase

- `make lint` (ruff check, ruff format --check, mypy strict) and `make test-fast` (no JVM)
  anywhere; `make test-spark` on Linux or WSL only.
- `make demo` on fresh volumes, holding the machine's heavy lock, then `make down`.
- CI mirrors the same targets; the compose smoke job runs `make demo` on the runner.

## Memory budget (16 GB laptop)

Every long-running service has a memory limit in `compose.yaml`; the README lists them. Target
total under 6 GB with everything up.
