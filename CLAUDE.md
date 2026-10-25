# CLAUDE.md

Change data capture demo: Postgres -> Debezium -> Kafka -> Spark Structured Streaming -> Delta
Lake on S3-compatible storage. Public portfolio repo; all data is synthetic.

## Commands

- `make install`: `uv sync --all-extras`.
- `make lint`: ruff check, ruff format --check, mypy strict on `src` and `dashboard`, and
  `dbt parse` (same as the CI lint job).
- `make test-fast`: pytest without a JVM (the default selection deselects `spark` and
  `integration`).
- `make test-spark`: Spark tests. Linux, macOS, WSL or CI only, never Windows Python. On the
  owner's Windows machine:
  `wsl -d Ubuntu -e bash -lc 'cd /mnt/d/dev/Pessoal/cdc-streaming-demo && UV_PROJECT_ENVIRONMENT=$HOME/.venvs/cdc-streaming-demo make test-spark'`.
- `make proto`: recompile `src/cdc_demo/proto/orders_value.desc` from the `.proto` (dev group:
  `grpcio-tools`).
- `make merge`: run `cdc-demo merge` in the `spark-job` container (stack up).
- `make mssql-demo`: the SQL Server path end to end (`MSSQL_ACCEPT_EULA=Y` required; run it
  instead of `make demo`, never next to it). `make lag` and `make stats` measure the stack
  that is up.
- `make marts`: build the DuckDB marts with dbt in the `analytics` container (stack up).
- `make dashboard`: start the Streamlit dashboard (http://localhost:8501).
- `make demo`: fresh stack, seeded workload, `cdc-demo verify`, two merges, `verify-current`,
  `build-marts`, `verify-marts`; leaves the stack up.
  `make down` removes it with its volumes.

## Repo map

- `compose.yaml`: the stack; project name `cdc-streaming-demo`; host ports from `.env`.
- `docker/app.Dockerfile`: `mutator` target (no JVM), `spark` target (JRE, pyspark, Ivy
  cache baked by `cdc-demo spark-warmup`) and `analytics` target (dbt, DuckDB with its
  extensions baked in, Streamlit). `docker/connect.Dockerfile`: Debezium Connect plus
  Confluent's Protobuf converter.
- `postgres/initdb/`: schema, seeded data, `cdc_reader` role, publication.
- `debezium/orders-connector.json`: connector config, registered by `scripts/register-connector.sh`.
- `src/cdc_demo/`: `cli.py` (Typer), `settings.py`, `mutator/`, `spark/session.py`,
  `ingest/` (schemas, parse, foreachBatch writer, streaming job), `merge/` (current table
  merger: schemas, `run_merge`), `proto/` (the Protobuf contract), `contract.py`
  (`check-schema`), `verify.py`, `verify_current.py` (`verify-current`), `marts.py`
  (`build-marts`), `verify_marts.py` (`verify-marts`), `spark/tables.py` (table creation
  shared by the consumer and the merger).
- `mssql/init/01_setup.sql`: SQL Server database, table, seed, CDC and logins (profile
  `mssql`). `src/cdc_demo/mssql/`: `lsn.py`, `mapping.py`, `writer.py`, `job.py`,
  `source_db.py`; `mutator/mssql_db.py`; `lag.py` (`lag-report`).
- `dbt/`: dbt-duckdb project `cdc_demo` (staging views, marts with enforced contracts, tests).
  `dashboard/app.py`: Streamlit app over the DuckDB file.
- `docs/PLAN.md`: layers, contracts, runtime contract, phases. `docs/DECISIONS.md`: decision log.

## Invariants

- The raw CDC log is append-only. Nothing updates or deletes its rows.
- `event_id` is `<topic>-<partition>-<offset>`; never random.
- Every Delta write in the consumer carries `txnAppId=CONSUMER_APP_ID` and
  `txnVersion=batch_id`. Resetting the checkpoint without resetting the tables requires a new
  `CONSUMER_APP_ID`, or Delta will skip the new batches as already written.
- `src/cdc_demo/proto/orders_value.proto`, `orders_value.desc` and the schema registered by
  Debezium change together: edit the `.proto`, run `make proto`, run `make demo` (its
  `check-schema` step fails on drift). Never hand-edit the `.desc`.
- `current/orders` is written only by `cdc-demo merge`. The merge watermark moves only after the
  MERGE commits, and the MERGE updates a row only when the event is newer than the stored one
  by `(source_lsn, kafka_offset)`. The merger reads the CDC log once, at a pinned version.
- The warehouse file `analytics.duckdb` is written only by `cdc-demo build-marts`: dbt builds
  into `analytics_build.duckdb` and the file is renamed into place after `dbt build` passed.
  Nothing else opens it for writing; the dashboard opens it read-only. DuckDB reads the lake
  through `s3://` (never `s3a://`, whose secrets it ignores): use `LakeSettings.duckdb_root`.
- `cdc_demo.cli`, `settings`, `logging`, `mutator` and `contract` never import pyspark, duckdb,
  dbt or streamlit at module level: the mutator image has none of them.
- dbt-common needs `protobuf<7`, so the dev group caps `protobuf` below 7 and `grpcio-tools`
  below 1.83; after a bump, rerun the descriptor test and `make proto` if it drifts.
- The SQL Server path writes only under `LAKE_ROOT=s3a://cdc-demo/mssql`, never under the
  Debezium root: ids and LSNs of the two sources are not comparable. Never run both stacks at
  once. `cdc_demo.cli` never imports `mssql_python` or `mssql_cdc` at module level (the
  mutator image has neither); `mssql_python` is imported inside the functions that connect.
  `MSSQL_ACCEPT_EULA` has no default anywhere except the CI workflow, where the owner accepts.
- The spark extra pins `pyspark` and `delta-spark` exactly; the Kafka, spark-protobuf and
  hadoop-aws package versions are derived from the installed pyspark, so bump them together.
- Env var names, CLI options and table paths are the contract in `docs/PLAN.md`; change the
  Python side and the compose side together.

## Conventions

- Python 3.12, uv, ruff (line length 100, `BLE`), mypy strict, pytest with Hypothesis.
- Settings through pydantic-settings; secrets as `SecretStr`, never logged.
- Logs are JSON lines with UTC timestamps and `run_id` / `batch_id`.
- Conventional Commits in English; one PR per phase (see `docs/PLAN.md`).
- Stack runs and Spark runs on the owner's machine take the shared heavy lock
  (`D:/dev/Pessoal/.locks/heavy`) and end with `make down`.
- No employer names, hosts or identifiers anywhere in this repo.
