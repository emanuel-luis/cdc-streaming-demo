from cdc_demo.mutator.model import Counts, Manifest
from cdc_demo.verify import compare, expected_from_manifest, format_table, poll


def manifest(c: int = 10, u: int = 5, d: int = 2) -> Manifest:
    now = "2026-10-25T12:00:00Z"
    return Manifest.model_validate(
        {
            "run_id": "r",
            "started_at": now,
            "finished_at": now,
            "seed": 1,
            "requested": 17,
            "counts": Counts(c=c, u=u, d=d).model_dump(),
            "skipped": 0,
        }
    )


def test_expected_from_manifest() -> None:
    e = expected_from_manifest(manifest(), 500)
    assert e["postgres_rows"] == 508
    assert (e["cdc_r"], e["cdc_c"], e["cdc_u"], e["cdc_d"]) == (500, 10, 5, 2)
    assert e["rejects"] == e["duplicate_event_ids"] == 0


def test_compare_ok_and_mismatches() -> None:
    e = expected_from_manifest(manifest(), 500)
    assert compare(e, dict(e)) == []
    bad = {**e, "cdc_u": 4, "rejects": 1}
    msgs = compare(e, bad)
    assert len(msgs) == 2
    assert "cdc_u: expected 5, got 4" in msgs
    del bad["cdc_d"]
    assert any("cdc_d" in m and "no value" in m for m in compare(e, bad))


def test_format_table_flags_mismatch() -> None:
    out = format_table({"a": 1, "b": 2}, {"a": 1, "b": 3})
    assert out.splitlines()[1].endswith("yes")
    assert out.splitlines()[2].endswith("NO")


def test_poll_waits_then_gives_up() -> None:
    e = {"a": 1}
    seq = iter([{"a": 0}, {"a": 1}])
    actual, problems = poll(e, lambda: next(seq), timeout=100, sleep=lambda _: None)
    assert problems == [] and actual == {"a": 1}

    ticks = iter(range(0, 100, 10))
    _, problems = poll(
        e, lambda: {"a": 0}, timeout=25, sleep=lambda _: None, clock=lambda: float(next(ticks))
    )
    assert problems
