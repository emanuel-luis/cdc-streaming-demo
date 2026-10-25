"""Build the DuckDB marts with dbt and publish the file only when the build passes.

dbt builds into a staging file next to the published one; a passing build is checkpointed and
renamed over `analytics.duckdb`, so a reader (the dashboard) never sees a half-built or failing
warehouse (decision 40).
"""

import json
import logging
import os
import subprocess
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

import duckdb

from cdc_demo.logging import log_event
from cdc_demo.settings import LakeSettings, WarehouseSettings

log = logging.getLogger(__name__)

Runner = Callable[..., Any]  # subprocess.run-like: returns an object with `returncode`


def build_marts(wh: WarehouseSettings, lake: LakeSettings, run: Runner = subprocess.run) -> int:
    """`dbt build` over the lake into the staging file; 0 only when the file was published."""
    run_id = uuid.uuid4().hex
    root = Path(wh.warehouse_dir)
    root.mkdir(parents=True, exist_ok=True)
    work = root / ".dbt"
    _remove_staging(wh)
    env = {
        **os.environ,
        "DBT_DUCKDB_PATH": str(wh.staging_path),
        "DBT_TARGET_PATH": str(work / "target"),
        "DBT_LOG_PATH": str(work / "logs"),
    }
    cmd = [
        "dbt",
        "build",
        "--project-dir",
        wh.dbt_project_dir,
        "--profiles-dir",
        wh.dbt_project_dir,
        "--vars",
        json.dumps({"lake_root": lake.duckdb_root}),
    ]
    log_event(log, "marts_build_start", run_id=run_id, lake_root=lake.duckdb_root)
    code = int(run(cmd, env=env, check=False).returncode)
    if code != 0:
        log_event(log, "marts_build_failed", run_id=run_id, exit_code=code)
        return code
    con = duckdb.connect(str(wh.staging_path))
    try:
        con.execute("CHECKPOINT")
    finally:
        con.close()
    os.replace(wh.staging_path, wh.path)
    log_event(log, "marts_published", run_id=run_id, path=str(wh.path))
    return 0


def _remove_staging(wh: WarehouseSettings) -> None:
    for stale in (wh.staging_path, wh.staging_path.with_name(wh.staging_path.name + ".wal")):
        stale.unlink(missing_ok=True)


def query(path: Path, sql: str) -> list[tuple[Any, ...]]:
    """Run one read-only query and return plain rows.

    The connection opens and closes per call, so the file can be replaced between calls.
    Select timestamps as `col::timestamp`: with the session time zone at UTC that is naive UTC.
    """
    con = duckdb.connect(str(path), read_only=True)
    try:
        con.execute("SET TimeZone = 'UTC'")
        return con.execute(sql).fetchall()
    finally:
        con.close()
