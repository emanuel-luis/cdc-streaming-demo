"""`cdc-demo` command line. Spark, dbt and DuckDB modules are imported inside the commands."""

from enum import StrEnum
from pathlib import Path
from typing import Annotated

import typer

from cdc_demo.logging import configure_logging
from cdc_demo.settings import LakeSettings, S3Settings, WarehouseSettings

app = typer.Typer(no_args_is_help=True, add_completion=False, help="CDC streaming demo.")


class Source(StrEnum):
    """The source database: Postgres (Debezium path) or SQL Server (direct Spark path)."""

    postgres = "postgres"
    mssql = "mssql"


@app.command()
def mutate(
    count: Annotated[int | None, typer.Option(min=1, help="Number of mutations.")] = None,
    duration: Annotated[float | None, typer.Option(min=0.1, help="Run for S seconds.")] = None,
    rate: Annotated[float | None, typer.Option(min=0.001, help="Mutations per second.")] = None,
    seed: Annotated[int | None, typer.Option(help="RNG seed; random when unset.")] = None,
    manifest: Annotated[Path, typer.Option(help="Where to write the run manifest.")] = Path(
        "runs/manifest.json"
    ),
    target: Annotated[
        Source, typer.Option(help="Database to mutate (mssql needs the spark image).")
    ] = Source.postgres,
) -> None:
    """Mutate orders in Postgres or SQL Server, one transaction per change, and write a manifest."""
    from cdc_demo.mutator.runner import run_mutations, write_manifest
    from cdc_demo.settings import MutatorSettings, PostgresSettings

    if count is not None and duration is not None:
        raise typer.BadParameter("use --count or --duration, not both")
    if count is None and duration is None:
        count = 100
    configure_logging()
    if target is Source.mssql:
        from cdc_demo.mutator.mssql_db import sqlserver_ops
        from cdc_demo.settings import SqlServerSettings

        pg, ops = None, sqlserver_ops(SqlServerSettings())
    else:
        pg, ops = PostgresSettings(), None
    result = run_mutations(
        pg,
        MutatorSettings(),
        count=count,
        duration=duration,
        rate=rate,
        seed=seed,
        ops=ops,
    )
    write_manifest(result, manifest)
    typer.echo(f"manifest written to {manifest}")


@app.command()
def consume(
    available_now: Annotated[
        bool, typer.Option("--available-now", help="Drain the topic, then stop.")
    ] = False,
) -> None:
    """Run the streaming job: Kafka -> raw CDC log, rejects and run metadata in Delta."""
    from cdc_demo.ingest.job import run_consumer
    from cdc_demo.settings import (
        ConsumerSettings,
        KafkaSettings,
        SparkSettings,
    )

    configure_logging()
    lake = LakeSettings()
    run_consumer(
        kafka=KafkaSettings(),
        lake=lake,
        consumer=ConsumerSettings(),
        spark_settings=SparkSettings(),
        s3=_s3_for(lake),
        available_now=available_now,
    )


@app.command()
def consume_mssql(
    available_now: Annotated[
        bool, typer.Option("--available-now", help="Drain the change table, then stop.")
    ] = False,
) -> None:
    """Run the SQL Server job: snapshot once, then mssql_cdc streaming into the CDC log."""
    from cdc_demo.mssql.job import run_mssql_consumer
    from cdc_demo.settings import ConsumerSettings, SparkSettings, SqlServerSettings

    configure_logging()
    lake = LakeSettings()
    run_mssql_consumer(
        sql=SqlServerSettings(),
        lake=lake,
        consumer=ConsumerSettings(),
        spark_settings=SparkSettings(),
        s3=_s3_for(lake),
        available_now=available_now,
    )


@app.command()
def verify(
    manifest: Annotated[Path, typer.Option(help="Manifest written by `mutate`.")],
    snapshot_rows: Annotated[int, typer.Option(min=0, help="Rows in the seeded snapshot.")] = 500,
    timeout: Annotated[float, typer.Option(min=0, help="Seconds to wait for the lake.")] = 300,
    source: Annotated[Source, typer.Option(help="Source database.")] = Source.postgres,
) -> None:
    """Poll the lake and the source until they match the manifest; exit 1 on timeout."""
    from cdc_demo.settings import PostgresSettings, SparkSettings, SqlServerSettings
    from cdc_demo.verify import run_verify

    configure_logging()
    lake = LakeSettings()
    code = run_verify(
        manifest,
        snapshot_rows,
        timeout,
        lake,
        PostgresSettings(),
        _s3_for(lake),
        SparkSettings(),
        sql=SqlServerSettings() if source is Source.mssql else None,
    )
    raise typer.Exit(code)


@app.command()
def merge() -> None:
    """Merge new CDC log events into the current orders table (batch, safe to re-run)."""
    from cdc_demo.merge.job import run_merge
    from cdc_demo.settings import SparkSettings
    from cdc_demo.spark.session import build_session

    configure_logging()
    lake = LakeSettings()
    spark = build_session(SparkSettings(), _s3_for(lake), kafka=False, app_name="cdc-demo-merge")
    try:
        result = run_merge(spark, lake)
    finally:
        spark.stop()
    typer.echo(result.summary())


@app.command()
def verify_current(
    timeout: Annotated[float, typer.Option(min=0, help="Seconds to wait for a match.")] = 0,
    source: Annotated[Source, typer.Option(help="Source database.")] = Source.postgres,
) -> None:
    """Compare the current table with the source row by row; exit 1 on any difference."""
    from cdc_demo.settings import PostgresSettings, SparkSettings, SqlServerSettings
    from cdc_demo.verify_current import run_verify_current

    configure_logging()
    lake = LakeSettings()
    code = run_verify_current(
        timeout,
        lake,
        PostgresSettings(),
        _s3_for(lake),
        SparkSettings(),
        sql=SqlServerSettings() if source is Source.mssql else None,
    )
    raise typer.Exit(code)


@app.command()
def lag_report() -> None:
    """Print p50, p95 and max lag (batch finish minus source commit) of the CDC log events."""
    from cdc_demo.lag import run_lag_report
    from cdc_demo.settings import SparkSettings

    configure_logging()
    lake = LakeSettings()
    raise typer.Exit(run_lag_report(lake, _s3_for(lake), SparkSettings()))


@app.command()
def build_marts() -> None:
    """Build the DuckDB marts with dbt from the Delta tables; publish only when all tests pass."""
    from cdc_demo.marts import build_marts as run_build

    configure_logging()
    raise typer.Exit(run_build(WarehouseSettings(), LakeSettings()))


@app.command()
def verify_marts(
    manifest: Annotated[Path, typer.Option(help="Manifest written by `mutate`.")],
    snapshot_rows: Annotated[int, typer.Option(min=0, help="Rows in the seeded snapshot.")] = 500,
) -> None:
    """Check the marts against the manifest; exit 1 on any difference."""
    from cdc_demo.verify_marts import run_verify_marts

    raise typer.Exit(run_verify_marts(manifest, snapshot_rows, WarehouseSettings()))


@app.command()
def check_schema() -> None:
    """Compare the checked-in Protobuf schema with the registered one; exit 1 on any difference."""
    from cdc_demo.contract import RegistryError, format_report
    from cdc_demo.contract import check_schema as run_check
    from cdc_demo.settings import SchemaRegistrySettings

    settings = SchemaRegistrySettings()
    try:
        result = run_check(settings.schema_registry_url, settings.value_subject)
    except RegistryError as exc:
        typer.echo(f"schema registry check failed: {exc}", err=True)
        raise typer.Exit(1) from exc
    typer.echo(format_report(result))
    raise typer.Exit(0 if result.ok else 1)


@app.command(hidden=True)
def spark_warmup() -> None:
    """Resolve every Spark package (Delta, Kafka, Protobuf, hadoop-aws) so Ivy caches the jars."""
    from cdc_demo.settings import SparkSettings
    from cdc_demo.spark.session import build_session

    configure_logging()
    build_session(
        SparkSettings(), kafka=True, protobuf=True, hadoop_aws=True, app_name="cdc-demo-warmup"
    ).stop()


def _s3_for(lake: LakeSettings) -> S3Settings | None:
    # A file:// or plain-path lake root (tests) needs no S3 settings.
    if lake.lake_root.startswith(("s3a://", "s3://", "s3n://")):
        return S3Settings()
    return None
