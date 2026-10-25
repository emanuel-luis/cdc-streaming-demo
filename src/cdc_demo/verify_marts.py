"""End-to-end assertions on the marts: the same manifest check as `verify`, read from DuckDB."""

import sys
from pathlib import Path

from cdc_demo.marts import query
from cdc_demo.mutator.model import Manifest
from cdc_demo.settings import WarehouseSettings
from cdc_demo.verify import compare, expected_from_manifest, format_table

KEYS = ("cdc_r", "cdc_c", "cdc_u", "cdc_d", "rejects", "live_orders")


def expected_marts(manifest: Manifest, snapshot_rows: int) -> dict[str, int]:
    full = expected_from_manifest(manifest, snapshot_rows)
    return {
        **{k: full[k] for k in KEYS if k in full},
        "live_orders": full["postgres_rows"],
    }


def collect_actuals(path: Path) -> dict[str, int]:
    actual = dict.fromkeys(KEYS, 0)
    events = query(
        path, "select op, sum(events)::bigint from marts.fct_order_activity_hourly group by op"
    )
    for op, n in events:
        actual[f"cdc_{op}"] = int(n)
    (rejects,) = query(
        path,
        "select coalesce(sum(rejected_rows), 0)::bigint from marts.fct_pipeline_runs "
        "where component = 'ingest'",
    )[0]
    actual["rejects"] = int(rejects)
    actual["live_orders"] = int(query(path, "select count(*) from marts.dim_orders_current")[0][0])
    return actual


def run_verify_marts(manifest_path: Path, snapshot_rows: int, wh: WarehouseSettings) -> int:
    """Exit code: 0 when the marts match the mutator manifest, 1 otherwise."""
    manifest = Manifest.model_validate_json(manifest_path.read_text(encoding="utf-8"))
    expected = expected_marts(manifest, snapshot_rows)
    if not wh.path.exists():
        sys.stdout.write(f"MISMATCH: no warehouse file at {wh.path}; run build-marts first\n")
        return 1
    actual = collect_actuals(wh.path)
    sys.stdout.write(format_table(expected, actual) + "\n")
    problems = compare(expected, actual)
    if problems:
        sys.stdout.write("\nMISMATCH:\n" + "\n".join(problems) + "\n")
        return 1
    sys.stdout.write("\nOK: the marts match the mutator manifest\n")
    return 0
