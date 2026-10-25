"""`cdc-demo` command line. Spark modules are imported inside the commands that need them."""

from pathlib import Path
from typing import Annotated

import typer

from cdc_demo.logging import configure_logging
from cdc_demo.settings import LakeSettings, S3Settings

app = typer.Typer(no_args_is_help=True, add_completion=False, help="CDC streaming demo.")


@app.command()
def mutate(
    count: Annotated[int | None, typer.Option(min=1, help="Number of mutations.")] = None,
    duration: Annotated[float | None, typer.Option(min=0.1, help="Run for S seconds.")] = None,
    rate: Annotated[float | None, typer.Option(min=0.001, help="Mutations per second.")] = None,
    seed: Annotated[int | None, typer.Option(help="RNG seed; random when unset.")] = None,
    manifest: Annotated[Path, typer.Option(help="Where to write the run manifest.")] = Path(
        "runs/manifest.json"
    ),
) -> None:
    """Mutate Postgres.orders, one transaction per change, and write a manifest."""
    from cdc_demo.mutator.runner import run_mutations, write_manifest
    from cdc_demo.settings import MutatorSettings, PostgresSettings

    if count is not None and duration is not None:
        raise typer.BadParameter("use --count or --duration, not both")
    if count is None and duration is None:
        count = 100
    configure_logging()
    result = run_mutations(
        PostgresSettings(),
        MutatorSettings(),
        count=count,
        duration=duration,
        rate=rate,
        seed=seed,
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
def verify(
    manifest: Annotated[Path, typer.Option(help="Manifest written by `mutate`.")],
    snapshot_rows: Annotated[int, typer.Option(min=0, help="Rows in the seeded snapshot.")] = 500,
    timeout: Annotated[float, typer.Option(min=0, help="Seconds to wait for the lake.")] = 300,
) -> None:
    """Poll the lake and Postgres until they match the manifest; exit 1 on timeout."""
    from cdc_demo.settings import PostgresSettings, SparkSettings
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
) -> None:
    """Compare the current table with Postgres row by row; exit 1 on any difference."""
    from cdc_demo.settings import PostgresSettings, SparkSettings
    from cdc_demo.verify_current import run_verify_current

    configure_logging()
    lake = LakeSettings()
    code = run_verify_current(timeout, lake, PostgresSettings(), _s3_for(lake), SparkSettings())
    raise typer.Exit(code)


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
