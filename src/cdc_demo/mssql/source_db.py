"""Reads from the SQL Server source for the verifiers. `mssql_python` is imported inside the
functions: this module loads in images that do not have the driver."""

from datetime import UTC, datetime

from cdc_demo.settings import SqlServerSettings
from cdc_demo.verify_current import Fields, Rows


def count_orders(sql: SqlServerSettings) -> int:
    import mssql_python

    conn = mssql_python.connect(autocommit=True, **sql.connect_kwargs())
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM dbo.orders")
        row = cur.fetchone()
        assert row is not None
        return int(row[0])
    finally:
        conn.close()


def _utc(value: datetime | None) -> datetime | None:
    # DATETIME2 comes back naive; the table is written with SYSUTCDATETIME().
    return None if value is None else value.replace(tzinfo=UTC)


def fetch_orders(sql: SqlServerSettings) -> Rows:
    import mssql_python

    query = "SELECT id, customer_id, status, amount, created_at, updated_at FROM dbo.orders"
    conn = mssql_python.connect(autocommit=True, **sql.connect_kwargs())
    try:
        cur = conn.cursor()
        cur.execute(query)
        out: Rows = {}
        for r in cur.fetchall():
            fields: Fields = (r[1], r[2], r[3], _utc(r[4]), _utc(r[5]))
            out[int(r[0])] = fields
        return out
    finally:
        conn.close()
