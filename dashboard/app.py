"""Read-only dashboard over the DuckDB marts that `cdc-demo build-marts` publishes."""

from datetime import UTC, datetime
from typing import Any

import streamlit as st

from cdc_demo.marts import query
from cdc_demo.settings import WarehouseSettings

MARTS = "marts"


def _hourly(rows: list[tuple[Any, ...]]) -> dict[str, dict[str, int]]:
    """{op: {hour: events}}, the shape st.bar_chart turns into one series per op."""
    chart: dict[str, dict[str, int]] = {}
    for hour, op, events in rows:
        chart.setdefault(op, {})[f"{hour:%m-%d %H:00}"] = int(events)
    return chart


def main() -> None:
    st.set_page_config(page_title="CDC demo", layout="wide")
    st.title("Orders CDC pipeline")
    path = WarehouseSettings().path
    if not path.exists():
        st.info("No warehouse yet. Run `make marts` to build it from the Delta tables.")
        return

    def q(sql: str) -> list[tuple[Any, ...]]:
        return query(path, sql)

    built = datetime.fromtimestamp(path.stat().st_mtime, UTC)
    st.caption(
        f"Marts built {built:%Y-%m-%d %H:%M:%S} UTC. Rebuild with `make marts`, then reload."
    )

    live = q(f"select count(*) from {MARTS}.dim_orders_current")[0][0]
    events = q(f"select coalesce(sum(events), 0)::bigint from {MARTS}.fct_order_activity_hourly")
    rejects = q(
        f"select coalesce(sum(rejected_rows), 0)::bigint from {MARTS}.fct_pipeline_runs "
        "where component = 'ingest'"
    )
    merged = q(
        f"select max(finished_at)::timestamp from {MARTS}.fct_pipeline_runs "
        "where component = 'merge'"
    )[0][0]
    cols = st.columns(4)
    cols[0].metric("Live orders", live)
    cols[1].metric("CDC events", events[0][0])
    cols[2].metric("Rejected messages", rejects[0][0])
    cols[3].metric("Last merge (UTC)", f"{merged:%H:%M:%S}" if merged else "never")

    left, right = st.columns(2)
    with left:
        st.subheader("Orders by status")
        by_status = q(
            f"select status, count(*) from {MARTS}.dim_orders_current group by status order by 1"
        )
        st.bar_chart({status: n for status, n in by_status})
    with right:
        st.subheader("CDC events per hour")
        hourly = q(
            f"select activity_hour::timestamp, op, events from {MARTS}.fct_order_activity_hourly "
            "where activity_hour is not null order by 1"
        )
        st.bar_chart(_hourly(hourly))

    st.subheader("Recent status changes")
    changes = q(
        "select changed_at::timestamp as changed_at, order_id, op, from_status, to_status "
        f"from {MARTS}.fct_order_status_changes order by changed_at desc, source_lsn desc limit 20"
    )
    st.dataframe(
        [dict(zip(("changed_at", "order_id", "op", "from", "to"), r, strict=True)) for r in changes]
    )

    st.subheader("Latest pipeline runs")
    names = (
        "component",
        "run_id",
        "batch_id",
        "started_at",
        "seconds",
        "input_rows",
        "output_rows",
        "rejected_rows",
    )
    runs = q(
        "select component, run_id, batch_id, started_at::timestamp, round(duration_seconds, 2), "
        f"input_rows, output_rows, rejected_rows from {MARTS}.fct_pipeline_runs "
        "order by started_at desc limit 10"
    )
    st.dataframe([dict(zip(names, r, strict=True)) for r in runs])


main()
