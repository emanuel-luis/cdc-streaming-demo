from pathlib import Path

import duckdb
import pytest

from cdc_demo.mutator.model import Counts, Manifest
from cdc_demo.settings import WarehouseSettings
from cdc_demo.verify_marts import collect_actuals, expected_marts, run_verify_marts


def manifest(c: int = 2, u: int = 2, d: int = 1) -> Manifest:
    now = "2026-10-25T12:00:00Z"
    return Manifest.model_validate(
        {
            "run_id": "r",
            "started_at": now,
            "finished_at": now,
            "seed": 1,
            "requested": c + u + d,
            "counts": Counts(c=c, u=u, d=d).model_dump(),
        }
    )


def write(tmp_path: Path, m: Manifest) -> Path:
    path = tmp_path / "manifest.json"
    path.write_text(m.model_dump_json(), encoding="utf-8")
    return path


def test_expected_marts() -> None:
    assert expected_marts(manifest(), 3) == {
        "cdc_r": 3,
        "cdc_c": 2,
        "cdc_u": 2,
        "cdc_d": 1,
        "rejects": 0,
        "live_orders": 4,
    }


def test_collect_actuals_reads_the_marts(warehouse_file: Path) -> None:
    assert collect_actuals(warehouse_file) == {
        "cdc_r": 3,
        "cdc_c": 2,
        "cdc_u": 2,
        "cdc_d": 1,
        "rejects": 1,
        "live_orders": 4,
    }


def test_matching_marts_pass(
    warehouse_file: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    con = duckdb.connect(str(warehouse_file))
    con.execute("update marts.fct_pipeline_runs set rejected_rows = 0")  # manifests expect none
    con.close()
    wh = WarehouseSettings(warehouse_dir=str(warehouse_file.parent))
    assert run_verify_marts(write(tmp_path, manifest()), 3, wh) == 0
    assert "OK" in capsys.readouterr().out


def test_unexpected_reject_fails(
    warehouse_file: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    wh = WarehouseSettings(warehouse_dir=str(warehouse_file.parent))
    assert run_verify_marts(write(tmp_path, manifest()), 3, wh) == 1
    assert "rejects: expected 0, got 1" in capsys.readouterr().out


def test_wrong_counts_fail(
    warehouse_file: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    wh = WarehouseSettings(warehouse_dir=str(warehouse_file.parent))
    assert run_verify_marts(write(tmp_path, manifest(c=5)), 3, wh) == 1
    out = capsys.readouterr().out
    assert "cdc_c: expected 5, got 2" in out
    assert "live_orders: expected 7, got 4" in out


def test_missing_warehouse_fails(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    wh = WarehouseSettings(warehouse_dir=str(tmp_path / "none"))
    assert run_verify_marts(write(tmp_path, manifest()), 3, wh) == 1
    assert "run build-marts first" in capsys.readouterr().out
