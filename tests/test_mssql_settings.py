from pathlib import Path

import pytest

from cdc_demo.settings import SqlServerSettings


@pytest.fixture(autouse=True)
def _isolate(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)  # no stray .env


def test_env_names(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SQLSERVER_HOST", "mssql")
    monkeypatch.setenv("SQLSERVER_PORT", "14330")
    s = SqlServerSettings()
    assert (s.host, s.port, s.db) == ("mssql", 14330, "cdc_demo")
    assert s.connection_string() == (
        "Server=mssql,14330;Database=cdc_demo;Encrypt=yes;TrustServerCertificate=yes"
    )


def test_secrets_stay_out_of_repr_and_connection_string(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SQLSERVER_PASSWORD", "Owner_secret_1!")
    monkeypatch.setenv("SQLSERVER_CDC_PASSWORD", "Reader_secret_1!")
    s = SqlServerSettings()
    for secret in ("Owner_secret_1!", "Reader_secret_1!"):
        assert secret not in repr(s) and secret not in s.connection_string()


def test_source_options_log_in_as_the_reader(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SQLSERVER_CDC_PASSWORD", "Reader_secret_1!")
    options = SqlServerSettings().source_options()
    assert options["user"] == "cdc_reader" and options["password"] == "Reader_secret_1!"
    assert options["captureInstance"] == "dbo_orders"
    assert "Reader_secret_1!" not in options["connectionString"]


def test_connect_kwargs_log_in_as_the_owner() -> None:
    kwargs = SqlServerSettings().connect_kwargs()
    assert kwargs["UID"] == "cdc_owner" and kwargs["connection_str"].startswith("Server=")
