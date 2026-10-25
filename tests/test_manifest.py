from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from cdc_demo.mutator.model import Counts, Manifest
from cdc_demo.mutator.runner import write_manifest


def make() -> Manifest:
    return Manifest(
        run_id="abc",
        started_at=datetime(2026, 10, 25, 12, 0, tzinfo=UTC),
        finished_at=datetime(2026, 10, 25, 12, 1, 30, 123456, tzinfo=UTC),
        seed=42,
        requested=200,
        counts=Counts(c=100, u=80, d=15),
        skipped=5,
    )


def test_round_trip_in_utc(tmp_path: Path) -> None:
    path = tmp_path / "sub" / "manifest.json"
    write_manifest(make(), path)
    text = path.read_text(encoding="utf-8")
    assert '"finished_at": "2026-10-25T12:01:30.123456Z"' in text
    loaded = Manifest.model_validate_json(text)
    assert loaded == make()
    offset = loaded.finished_at.utcoffset()
    assert offset is not None and offset.total_seconds() == 0


def test_naive_datetime_rejected() -> None:
    with pytest.raises(ValidationError):
        Manifest(
            run_id="x",
            started_at=datetime(2026, 1, 1),
            finished_at=datetime(2026, 1, 1, tzinfo=UTC),
            seed=1,
            requested=None,
        )
