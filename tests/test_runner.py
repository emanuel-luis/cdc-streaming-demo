from typing import Any

import psycopg
import pytest

from cdc_demo.mutator import db, runner
from cdc_demo.mutator.model import Manifest
from cdc_demo.settings import MutatorSettings, PostgresSettings


class FakeConn:
    def close(self) -> None:
        pass


def run(monkeypatch: pytest.MonkeyPatch, apply: Any, connects: list[int], **kw: Any) -> Manifest:
    monkeypatch.setattr(runner, "apply_op", apply)

    def connect(**_: Any) -> FakeConn:
        connects.append(1)
        return FakeConn()

    return runner.run_mutations(
        PostgresSettings(),
        MutatorSettings(),
        connect=connect,  # type: ignore[arg-type]
        sleep=lambda _: None,
        **kw,
    )


def test_counts_and_skips_are_reproducible(monkeypatch: pytest.MonkeyPatch) -> None:
    def apply(conn: Any, op: str, rng: Any, p: float) -> int | None:
        return None if op == "d" else 1

    a = run(monkeypatch, apply, [], count=50, seed=7)
    b = run(monkeypatch, apply, [], count=50, seed=7)
    assert a.counts == b.counts and a.skipped == b.skipped
    assert a.counts.c + a.counts.u + a.counts.d + a.skipped == 50
    assert a.counts.d == 0 and a.requested == 50 and a.seed == 7


def test_retries_operational_error_with_new_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"n": 0}

    def apply(conn: Any, op: str, rng: Any, p: float) -> int:
        calls["n"] += 1
        if calls["n"] <= 2:
            raise psycopg.OperationalError("down")
        return 1

    connects: list[int] = []
    m = run(monkeypatch, apply, connects, count=1, seed=1)
    assert len(connects) == 3
    assert m.counts.c + m.counts.u + m.counts.d == 1


def test_retry_replays_the_same_random_choices(monkeypatch: pytest.MonkeyPatch) -> None:
    draws: list[float] = []

    def apply(conn: Any, op: str, rng: Any, p: float) -> int:
        draws.append(rng.random())
        if len(draws) == 1:
            raise psycopg.OperationalError("down")
        return 1

    run(monkeypatch, apply, [], count=1, seed=3)
    assert len(draws) == 2 and draws[0] == draws[1]


def test_retried_run_matches_clean_run(monkeypatch: pytest.MonkeyPatch) -> None:
    def recorder(fail_at: int | None) -> tuple[Any, list[float]]:
        seen: list[float] = []
        state = {"calls": 0}

        def apply(conn: Any, op: str, rng: Any, p: float) -> int:
            state["calls"] += 1
            value = rng.random()
            if state["calls"] == fail_at:
                raise psycopg.OperationalError("down")
            seen.append(value)
            return 1

        return apply, seen

    clean, clean_seen = recorder(None)
    flaky, flaky_seen = recorder(4)
    a = run(monkeypatch, clean, [], count=10, seed=11)
    b = run(monkeypatch, flaky, [], count=10, seed=11)
    assert clean_seen == flaky_seen
    assert a.counts == b.counts


def test_commit_outcome_unknown_is_not_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    def apply(conn: Any, op: str, rng: Any, p: float) -> int:
        raise db.CommitOutcomeUnknownError("lost during commit")

    connects: list[int] = []
    with pytest.raises(db.CommitOutcomeUnknownError):
        run(monkeypatch, apply, connects, count=3, seed=1)
    assert len(connects) == 1


def test_needs_exactly_one_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(ValueError):
        run(monkeypatch, None, [], seed=1)


def test_loop_runs_on_a_db_ops_bundle(monkeypatch: pytest.MonkeyPatch) -> None:
    class Down(Exception):
        pass

    calls: list[str] = []

    def apply(conn: Any, op: str, rng: Any, p: float) -> int:
        calls.append(op)
        if len(calls) == 1:
            raise Down("dropped")
        return 1

    connects: list[int] = []
    ops = runner.DbOps(
        connect=lambda: connects.append(1) or FakeConn(), apply_op=apply, transient=(Down,)
    )
    m = runner.run_mutations(
        None, MutatorSettings(), count=2, seed=5, ops=ops, sleep=lambda _: None
    )
    assert len(connects) == 2 and m.counts.c + m.counts.u + m.counts.d == 2
    with pytest.raises(ValueError, match="db ops"):
        runner.run_mutations(None, MutatorSettings(), count=1, seed=5)
