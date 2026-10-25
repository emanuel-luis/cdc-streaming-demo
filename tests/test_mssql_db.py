import random
import subprocess
import sys
from decimal import Decimal
from typing import Any

import pytest

from cdc_demo.mutator import mssql_db
from cdc_demo.mutator.db import CommitOutcomeUnknownError


class DriverError(Exception):
    """Stands in for mssql_python.OperationalError."""


class FakeCursor:
    def __init__(self, conn: "FakeConn") -> None:
        self.conn = conn
        self.rows: list[tuple[Any, ...]] = []

    def execute(self, sql: str, *params: Any) -> None:
        self.conn.statements.append((sql, params))
        if sql.startswith("SELECT id, status"):
            self.rows = self.conn.candidates
        elif "OUTPUT INSERTED.id" in sql:
            self.rows = [(77,)]

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.rows

    def fetchone(self) -> tuple[Any, ...] | None:
        return self.rows[0] if self.rows else None

    def close(self) -> None:
        pass


class FakeConn:
    def __init__(self, candidates: list[tuple[int, str]], commit_error: Exception | None = None):
        self.candidates: list[tuple[Any, ...]] = list(candidates)
        self.commit_error = commit_error
        self.statements: list[tuple[str, tuple[Any, ...]]] = []
        self.commits = 0
        self.rollbacks = 0

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def commit(self) -> None:
        if self.commit_error:
            raise self.commit_error
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


@pytest.fixture(autouse=True)
def _no_driver(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mssql_db, "transient_errors", lambda: (DriverError,))


def apply(conn: FakeConn, op: Any = "c") -> int | None:
    return mssql_db.apply_op(conn, op, random.Random(1), 0.1)


def test_insert_uses_output_and_utc_clock_and_commits_once() -> None:
    conn = FakeConn([])
    assert apply(conn) == 77
    sql, params = conn.statements[0]
    assert "OUTPUT INSERTED.id" in sql and sql.count("SYSUTCDATETIME()") == 2
    assert params[1] == "pending" and isinstance(params[2], Decimal)
    assert conn.commits == 1 and conn.rollbacks == 0


def test_update_sets_updated_at_and_guards_on_the_old_status() -> None:
    conn = FakeConn([(5, "confirmed")])
    assert apply(conn, "u") == 5
    sql, params = conn.statements[-1]
    assert sql.startswith("UPDATE dbo.orders") and "updated_at = SYSUTCDATETIME()" in sql
    assert params[-2:] == (5, "confirmed")
    # The candidate query lists one placeholder per status, never a Python-formatted value.
    pick_sql, pick_params = conn.statements[0]
    assert pick_sql.count("?") == 3 == len(pick_params)


def test_delete_picks_only_terminal_orders() -> None:
    conn = FakeConn([(9, "delivered")])
    assert apply(conn, "d") == 9
    sql, params = conn.statements[-1]
    assert sql.startswith("DELETE FROM dbo.orders") and sql.count("?") == 3
    assert params == (9, "delivered", "cancelled")


def test_nothing_to_pick_is_skipped_but_still_commits() -> None:
    conn = FakeConn([])
    assert apply(conn, "u") is None
    assert conn.commits == 1


def test_error_before_commit_rolls_back_and_propagates() -> None:
    conn = FakeConn([])

    def broken(cur: Any, rng: Any) -> int:
        raise DriverError("dropped mid-statement")

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(mssql_db, "insert_order", broken)
        with pytest.raises(DriverError):
            apply(conn)
    assert conn.rollbacks == 1 and conn.commits == 0


def test_error_during_commit_is_unknown_outcome() -> None:
    conn = FakeConn([], commit_error=DriverError("lost"))
    with pytest.raises(CommitOutcomeUnknownError):
        apply(conn)


def test_modules_load_without_driver_spark_or_cdc_library() -> None:
    code = (
        "import sys\n"
        "for name in ('mssql_python', 'mssql_cdc', 'pyspark'):\n"
        "    sys.modules[name] = None\n"
        "import cdc_demo.cli, cdc_demo.settings, cdc_demo.mutator.mssql_db, cdc_demo.lag\n"
    )
    done = subprocess.run(
        [sys.executable, "-I", "-c", code], check=False, capture_output=True, text=True
    )
    assert done.returncode == 0, done.stderr
