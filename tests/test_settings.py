from pathlib import Path

import pytest
from pydantic import ValidationError

from cdc_demo.settings import (
    LakeSettings,
    MutatorSettings,
    PostgresSettings,
    S3Settings,
    WarehouseSettings,
)


@pytest.fixture(autouse=True)
def _isolate(monkeypatch: pytest.MonkeyPatch, tmp_path: pytest.TempPathFactory) -> None:
    monkeypatch.chdir(tmp_path)  # no stray .env


def test_defaults_match_the_contract() -> None:
    assert PostgresSettings().port == 5432
    m = MutatorSettings()
    assert (m.rate, m.seed, m.cancel_probability) == (20, None, 0.1)


def test_env_names(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_HOST", "db")
    monkeypatch.setenv("MUTATOR_SEED", "42")
    monkeypatch.setenv("LAKE_ROOT", "file:///tmp/lake/")
    assert PostgresSettings().host == "db"
    assert MutatorSettings().seed == 42
    lake = LakeSettings()
    assert lake.cdc_log == "file:///tmp/lake/raw/orders_cdc_log"
    assert lake.rejects == "file:///tmp/lake/raw/_rejects/orders_cdc_log"
    assert lake.ingest_runs == "file:///tmp/lake/_meta/ingest_runs"
    assert lake.checkpoint("q") == "file:///tmp/lake/_checkpoints/q"


@pytest.mark.parametrize(
    "env",
    [
        {"MUTATOR_RATE": "0"},
        {"MUTATOR_INSERT_WEIGHT": "-1"},
        {"MUTATOR_CANCEL_PROBABILITY": "1.5"},
        {"MUTATOR_INSERT_WEIGHT": "0", "MUTATOR_UPDATE_WEIGHT": "0", "MUTATOR_DELETE_WEIGHT": "0"},
    ],
)
def test_mutator_validation(monkeypatch: pytest.MonkeyPatch, env: dict[str, str]) -> None:
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    with pytest.raises(ValidationError):
        MutatorSettings()


def test_secrets_not_in_repr(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_PASSWORD", "s3cr3t-pg")
    monkeypatch.setenv("S3_SECRET_KEY", "s3cr3t-s3")
    pg, s3 = PostgresSettings(), S3Settings()
    for obj in (pg, s3):
        assert "s3cr3t" not in repr(obj)
        assert "s3cr3t" not in str(obj)
    assert pg.connect_kwargs()["password"] == "s3cr3t-pg"


@pytest.mark.parametrize(
    ("root", "want"),
    [
        ("s3a://cdc-demo", "s3://cdc-demo"),
        ("s3a://cdc-demo/", "s3://cdc-demo"),
        ("s3://cdc-demo", "s3://cdc-demo"),
        ("file:///tmp/lake/", "file:///tmp/lake"),
    ],
)
def test_duckdb_root(root: str, want: str) -> None:
    assert LakeSettings(lake_root=root).duckdb_root == want


def test_warehouse_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    wh = WarehouseSettings()
    assert (wh.warehouse_dir, wh.dbt_project_dir) == ("warehouse", "dbt")
    assert wh.path == Path("warehouse/analytics.duckdb")
    monkeypatch.setenv("WAREHOUSE_DIR", "/warehouse")
    monkeypatch.setenv("DBT_PROJECT_DIR", "/opt/dbt")
    wh = WarehouseSettings()
    assert wh.path == Path("/warehouse/analytics.duckdb")
    assert wh.staging_path == Path("/warehouse/analytics.build.duckdb")
    assert wh.dbt_project_dir == "/opt/dbt"
