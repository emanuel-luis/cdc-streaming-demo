from typing import Any

import psycopg
import pytest

from cdc_demo.mutator import runner
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


def test_needs_exactly_one_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(ValueError):
        run(monkeypatch, None, [], seed=1)
