"""Environment-driven settings, one class per component. Names follow docs/PLAN.md."""

from pathlib import Path
from typing import Any

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _config(prefix: str = "") -> SettingsConfigDict:
    return SettingsConfigDict(
        env_prefix=prefix, env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )


class PostgresSettings(BaseSettings):
    model_config = _config("POSTGRES_")

    host: str = "localhost"
    port: int = 5432
    db: str = "cdc_demo"
    user: str = "cdc_owner"
    password: SecretStr = SecretStr("cdc_owner_pw")

    def connect_kwargs(self) -> dict[str, Any]:
        # Keyword arguments instead of a DSN string: nothing here can be logged by accident.
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.db,
            "user": self.user,
            "password": self.password.get_secret_value(),
        }


class MutatorSettings(BaseSettings):
    model_config = _config("MUTATOR_")

    rate: float = Field(default=20.0, gt=0)
    seed: int | None = None
    insert_weight: float = Field(default=0.5, ge=0)
    update_weight: float = Field(default=0.4, ge=0)
    delete_weight: float = Field(default=0.1, ge=0)
    cancel_probability: float = Field(default=0.1, ge=0, le=1)

    @model_validator(mode="after")
    def _weights_sum_positive(self) -> "MutatorSettings":
        if self.insert_weight + self.update_weight + self.delete_weight <= 0:
            raise ValueError("operation weights must have a positive sum")
        return self


class S3Settings(BaseSettings):
    model_config = _config("S3_")

    endpoint: str = "http://localhost:9000"
    access_key: str = "cdc_demo_admin"
    secret_key: SecretStr = SecretStr("cdc_demo_secret")
    region: str = "us-east-1"


class LakeSettings(BaseSettings):
    """Where the Delta tables live. A file:// or plain path works in tests."""

    model_config = _config()

    lake_root: str = "s3a://cdc-demo"

    def _path(self, *parts: str) -> str:
        return "/".join([self.lake_root.rstrip("/"), *parts])

    @property
    def cdc_log(self) -> str:
        return self._path("raw", "orders_cdc_log")

    @property
    def rejects(self) -> str:
        return self._path("raw", "_rejects", "orders_cdc_log")

    @property
    def ingest_runs(self) -> str:
        return self._path("_meta", "ingest_runs")

    @property
    def current_orders(self) -> str:
        return self._path("current", "orders")

    @property
    def merge_watermark(self) -> str:
        return self._path("_meta", "merge_watermark")

    @property
    def merge_runs(self) -> str:
        return self._path("_meta", "merge_runs")

    def checkpoint(self, name: str) -> str:
        return self._path("_checkpoints", name)

    @property
    def duckdb_root(self) -> str:
        # DuckDB's S3 secrets apply to s3:// only; the s3a:// scheme is Hadoop's (decision 38).
        for scheme in ("s3a://", "s3n://"):
            if self.lake_root.startswith(scheme):
                return "s3://" + self.lake_root.removeprefix(scheme).rstrip("/")
        return self.lake_root.rstrip("/")


class WarehouseSettings(BaseSettings):
    """The DuckDB file the marts are built into and the dashboard reads."""

    model_config = _config()

    warehouse_dir: str = "warehouse"
    dbt_project_dir: str = "dbt"

    @property
    def path(self) -> Path:
        return Path(self.warehouse_dir) / "analytics.duckdb"

    @property
    def staging_path(self) -> Path:
        return Path(self.warehouse_dir) / "analytics.build.duckdb"


class KafkaSettings(BaseSettings):
    model_config = _config()

    kafka_bootstrap_servers: str = "localhost:9092"
    cdc_topic: str = "cdc.public.orders"


class SchemaRegistrySettings(BaseSettings):
    model_config = _config()

    schema_registry_url: str = "http://localhost:8081"
    cdc_topic: str = "cdc.public.orders"

    @property
    def value_subject(self) -> str:
        # Confluent's TopicNameStrategy: <topic>-value.
        return f"{self.cdc_topic}-value"


class ConsumerSettings(BaseSettings):
    model_config = _config("CONSUMER_")

    trigger_interval: str = "10 seconds"
    max_offsets_per_trigger: int = Field(default=10000, gt=0)
    app_id: str = "orders-cdc-consumer-v1"


class SparkSettings(BaseSettings):
    model_config = _config()

    spark_master: str = "local[2]"
    spark_driver_memory: str = "1g"
    spark_ivy_dir: str | None = None
