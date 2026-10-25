from pathlib import Path

import pytest

pytest.importorskip("streamlit")
from streamlit.testing.v1 import AppTest

APP = Path(__file__).parent.parent / "dashboard" / "app.py"


def run_app(warehouse_dir: Path, monkeypatch: pytest.MonkeyPatch) -> AppTest:
    monkeypatch.setenv("WAREHOUSE_DIR", str(warehouse_dir))
    return AppTest.from_file(str(APP), default_timeout=30).run()


def test_missing_warehouse_shows_a_hint(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    at = run_app(tmp_path / "none", monkeypatch)
    assert not at.exception
    assert "make marts" in at.info[0].value
    assert not at.metric


def test_renders_the_marts(warehouse_file: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    at = run_app(warehouse_file.parent, monkeypatch)
    assert not at.exception
    metrics = {m.label: m.value for m in at.metric}
    assert metrics["Live orders"] == "4"
    assert metrics["CDC events"] == "8"
    assert metrics["Rejected messages"] == "1"
    assert metrics["Last merge (UTC)"] == "12:10:05"
    assert "UTC" in at.caption[0].value
    assert len(at.dataframe) == 2
