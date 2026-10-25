# CLAUDE.md

Change data capture demo: Postgres -> Debezium -> Kafka -> Spark Structured Streaming -> Delta
Lake on S3-compatible storage. Public portfolio repo; all data is synthetic.

## Commands

- `make install`: `uv sync --all-extras`.
- `make lint`: ruff check, ruff format --check, mypy strict on `src` (same as the CI lint job).
- `make test-fast`: pytest without a JVM (the default selection deselects `spark` and
  `integration`).
- `make test-spark`: Spark tests. Linux, macOS, WSL or CI only, never Windows Python. On the
  owner's Windows machine:
  `wsl -d Ubuntu -e bash -lc 'cd /mnt/d/dev/Pessoal/cdc-streaming-demo && UV_PROJECT_ENVIRONMENT=$HOME/.venvs/cdc-streaming-demo make test-spark'`.
- `make demo`: fresh stack, seeded workload, `cdc-demo verify`; leaves the stack up.
  `make down` removes it with its volumes.

## Repo map

- `compose.yaml`: the stack; project name `cdc-streaming-demo`; host ports from `.env`.
- `docker/app.Dockerfile`: `mutator` target (no JVM) and `spark` target (JRE, pyspark, Ivy
  cache baked by `cdc-demo spark-warmup`).
- `postgres/initdb/`: schema, seeded data, `cdc_reader` role, publication.
- `debezium/orders-connector.json`: connector config, registered by `scripts/register-connector.sh`.
- `src/cdc_demo/`: `cli.py` (Typer), `settings.py`, `mutator/`, `spark/session.py`,
  `ingest/` (schemas, parse, foreachBatch writer, streaming job), `verify.py`.
- `docs/PLAN.md`: layers, contracts, runtime contract, phases. `docs/DECISIONS.md`: decision log.

## Invariants

- The raw CDC log is append-only. Nothing updates or deletes its rows.
- `event_id` is `<topic>-<partition>-<offset>`; never random.
- Every Delta write in the consumer carries `txnAppId=CONSUMER_APP_ID` and
  `txnVersion=batch_id`. Resetting the checkpoint without resetting the tables requires a new
  `CONSUMER_APP_ID`, or Delta will skip the new batches as already written.
- `cdc_demo.cli`, `settings`, `logging` and `mutator` never import pyspark at module level: the
  mutator image has no pyspark.
- The spark extra pins `pyspark` and `delta-spark` exactly; the Kafka and hadoop-aws package
  versions are derived from the installed pyspark, so bump them together.
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
