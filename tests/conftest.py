"""Shared test setup: tests import helpers such as `protobuf_support` from this directory."""

from pathlib import Path

import pytest

MARTS_SQL = Path(__file__).parent / "fixtures" / "marts.sql"


@pytest.fixture
def warehouse_file(tmp_path: Path) -> Path:
    """A DuckDB file holding the four marts (tests/fixtures/marts.sql)."""
    duckdb = pytest.importorskip("duckdb")
    path = tmp_path / "analytics.duckdb"
    con = duckdb.connect(str(path))
    try:
        con.execute(MARTS_SQL.read_text(encoding="utf-8"))
    finally:
        con.close()
    return path
