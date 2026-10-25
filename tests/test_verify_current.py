from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal

from cdc_demo.settings import LakeSettings
from cdc_demo.verify_current import (
    KeyedRows,
    RowDiff,
    Rows,
    compare_rows,
    from_micros,
    wait_for_match,
)

T0 = datetime(2026, 10, 25, 12, 0, tzinfo=UTC)


def row(
    status: str = "pending", amount: str = "10.00", updated: datetime = T0
) -> tuple[int, str, Decimal, datetime, datetime]:
    return (7, status, Decimal(amount), T0, updated)


def test_equal_tables_are_ok() -> None:
    rows: Rows = {1: row(), 2: row("shipped")}
    diff = compare_rows(list(rows.items()), dict(rows))
    assert diff.ok
    assert (diff.current_rows, diff.postgres_rows) == (2, 2)


def test_missing_extra_and_mismatched() -> None:
    current: KeyedRows = [(1, row()), (2, row()), (4, row("shipped"))]
    postgres: Rows = {1: row(), 2: row(amount="10.50"), 3: row()}
    diff = compare_rows(current, postgres)
    assert not diff.ok
    assert diff.missing_in_current == [3]
    assert diff.extra_in_current == [4]
    assert diff.mismatched == [2]
    assert diff.duplicate_keys == []


def test_duplicate_key_fails_even_when_every_row_matches() -> None:
    postgres: Rows = {1: row(), 2: row("shipped")}
    current: KeyedRows = [(1, row()), (2, row("shipped")), (1, row())]
    diff = compare_rows(current, postgres)
    assert diff.duplicate_keys == [1]
    assert (diff.current_rows, diff.postgres_rows) == (3, 2)
    assert (diff.missing_in_current, diff.extra_in_current, diff.mismatched) == ([], [], [])
    assert not diff.ok
    assert "duplicate_keys      1" in diff.report()
    assert "duplicate_keys examples: [1]" in diff.report()


def test_timestamps_compare_across_time_zones() -> None:
    brt = timezone(timedelta(hours=-3))
    same_instant = T0.astimezone(brt)
    assert compare_rows([(1, row(updated=T0))], {1: row(updated=same_instant)}).ok
    assert not compare_rows(
        [(1, row(updated=T0))], {1: row(updated=T0 + timedelta(microseconds=1))}
    ).ok


def test_report_lists_at_most_ten_examples() -> None:
    diff = RowDiff(0, 30, list(range(30)), [], [], [])
    text = diff.report()
    assert "missing_in_current  30" in text
    assert str(list(range(10))) in text
    assert "10," not in text.split("examples:")[1]
    assert "extra_in_current examples" not in text


def test_from_micros() -> None:
    assert from_micros(None) is None
    assert from_micros(0) == datetime(1970, 1, 1, tzinfo=UTC)
    assert from_micros(1_000_001) == datetime(1970, 1, 1, 0, 0, 1, 1, tzinfo=UTC)


def test_wait_for_match_retries_then_gives_up() -> None:
    bad = RowDiff(1, 2, [5], [], [], [])
    good = RowDiff(2, 2, [], [], [], [])
    seq = iter([bad, good])
    assert wait_for_match(lambda: next(seq), 100, sleep=lambda _: None).ok

    ticks = iter(range(0, 100, 10))
    last = wait_for_match(lambda: bad, 25, sleep=lambda _: None, clock=lambda: float(next(ticks)))
    assert not last.ok


def test_lake_paths() -> None:
    lake = LakeSettings(lake_root="file:///lake/")
    assert lake.current_orders == "file:///lake/current/orders"
    assert lake.merge_watermark == "file:///lake/_meta/merge_watermark"
    assert lake.merge_runs == "file:///lake/_meta/merge_runs"
