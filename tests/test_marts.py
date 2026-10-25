import subprocess
import sys
from pathlib import Path
from typing import Any

import duckdb

from cdc_demo.marts import build_marts, query
from cdc_demo.settings import LakeSettings, WarehouseSettings


def settings(tmp_path: Path) -> tuple[WarehouseSettings, LakeSettings]:
    return (
        WarehouseSettings(warehouse_dir=str(tmp_path / "wh"), dbt_project_dir="dbt"),
        LakeSettings(lake_root="s3a://cdc-demo"),
    )


def make_db(path: str, value: int) -> None:
    con = duckdb.connect(path)
    con.execute(f"create table t as select {value} as v")
    con.close()


def test_success_publishes_the_staging_file(tmp_path: Path) -> None:
    wh, lake = settings(tmp_path)
    seen: dict[str, Any] = {}

    def run(cmd: list[str], env: dict[str, str], check: bool) -> subprocess.CompletedProcess[str]:
        seen["cmd"], seen["env"] = cmd, env
        make_db(env["DBT_DUCKDB_PATH"], 2)
        return subprocess.CompletedProcess(cmd, 0)

    assert build_marts(wh, lake, run=run) == 0
    assert query(wh.path, "select v from t") == [(2,)]
    assert not wh.staging_path.exists()
    assert seen["cmd"][:2] == ["dbt", "build"]
    assert '"lake_root": "s3://cdc-demo"' in seen["cmd"][-1]
    assert seen["env"]["DBT_DUCKDB_PATH"] == str(wh.staging_path)
    assert seen["env"]["DBT_TARGET_PATH"].startswith(wh.warehouse_dir)


def test_failure_leaves_the_published_file_untouched(tmp_path: Path) -> None:
    wh, lake = settings(tmp_path)
    Path(wh.warehouse_dir).mkdir()
    make_db(str(wh.path), 1)

    def run(cmd: list[str], env: dict[str, str], check: bool) -> subprocess.CompletedProcess[str]:
        make_db(env["DBT_DUCKDB_PATH"], 99)
        return subprocess.CompletedProcess(cmd, 1)

    assert build_marts(wh, lake, run=run) == 1
    assert query(wh.path, "select v from t") == [(1,)]


def test_stale_staging_files_are_removed_before_the_build(tmp_path: Path) -> None:
    wh, lake = settings(tmp_path)
    Path(wh.warehouse_dir).mkdir()
    wal = wh.staging_path.with_name(wh.staging_path.name + ".wal")
    wh.staging_path.write_bytes(b"stale")
    wal.write_bytes(b"stale")
    present: list[bool] = []

    def run(cmd: list[str], env: dict[str, str], check: bool) -> subprocess.CompletedProcess[str]:
        present.append(wh.staging_path.exists() or wal.exists())
        return subprocess.CompletedProcess(cmd, 1)

    build_marts(wh, lake, run=run)
    assert present == [False]


def test_query_is_read_only_and_utc(warehouse_file: Path) -> None:
    rows = query(
        warehouse_file,
        "select created_at::timestamp from marts.dim_orders_current where order_id = 1",
    )
    assert str(rows[0][0]) == "2026-10-25 10:00:00"


def test_cli_imports_no_heavy_module() -> None:
    code = (
        "import sys, cdc_demo.cli;"
        "bad = {'duckdb', 'pyspark', 'streamlit', 'dbt'} & set(sys.modules);"
        "sys.exit(1 if bad else 0)"
    )
    assert subprocess.run([sys.executable, "-I", "-c", code], check=False).returncode == 0
