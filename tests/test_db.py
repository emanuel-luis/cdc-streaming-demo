import random
from typing import Any, Self

import psycopg
import pytest

from cdc_demo.mutator import db


class FakeCursor:
    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        pass


class FakeConn:
    def __init__(self, commit_error: Exception | None = None) -> None:
        self.commit_error = commit_error
        self.commits = 0
        self.rollbacks = 0

    def cursor(self) -> FakeCursor:
        return FakeCursor()

    def commit(self) -> None:
        if self.commit_error is not None:
            raise self.commit_error
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1
        raise psycopg.OperationalError("connection is gone")


def apply(conn: FakeConn) -> int | None:
    return db.apply_op(conn, "c", random.Random(1), 0.1)  # type: ignore[arg-type]


def test_success_commits_once(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(db, "insert_order", lambda cur, rng: 42)
    conn = FakeConn()
    assert apply(conn) == 42
    assert conn.commits == 1 and conn.rollbacks == 0


def test_error_before_commit_rolls_back_and_propagates(monkeypatch: pytest.MonkeyPatch) -> None:
    def broken(cur: Any, rng: Any) -> int:
        raise psycopg.OperationalError("dropped mid-statement")

    monkeypatch.setattr(db, "insert_order", broken)
    conn = FakeConn()
    # A failing rollback on a dead connection must not hide the original error.
    with pytest.raises(psycopg.OperationalError, match="mid-statement"):
        apply(conn)
    assert conn.rollbacks == 1 and conn.commits == 0


def test_error_during_commit_is_reported_as_unknown_outcome(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(db, "insert_order", lambda cur, rng: 42)
    conn = FakeConn(commit_error=psycopg.OperationalError("lost during commit"))
    with pytest.raises(db.CommitOutcomeUnknownError):
        apply(conn)
