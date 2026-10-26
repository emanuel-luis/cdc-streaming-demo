"""SQL Server access for the mutator, the counterpart of `mutator/db.py`.

Same mutations, same RNG use, so a seed makes the same changes on both sources. Parameters are
qmark placeholders; timestamps come from SYSUTCDATETIME() because `dbo.orders` has no trigger.
`mssql_python` is imported inside the functions: modules that only need the types load without
the driver.
"""

import contextlib
import random
from collections.abc import Sequence
from typing import Any

from cdc_demo.mutator.db import CommitOutcomeUnknownError
from cdc_demo.mutator.model import (
    CANCELLED,
    DELIVERED,
    PENDING,
    UPDATABLE,
    Op,
    next_status,
    random_amount,
    wants_amount_change,
)
from cdc_demo.mutator.runner import DbOps
from cdc_demo.settings import SqlServerSettings


def transient_errors() -> tuple[type[Exception], ...]:
    """Errors worth a reconnect: lost connections (SQLSTATE 08xxx), deadlocks (40001), timeouts."""
    import mssql_python

    return (mssql_python.OperationalError, mssql_python.InterfaceError)


def connect(settings: SqlServerSettings) -> Any:
    """A new connection with autocommit off, as the owner."""
    import mssql_python

    return mssql_python.connect(autocommit=False, **settings.connect_kwargs())


def _in(statuses: Sequence[str]) -> str:
    return ", ".join("?" * len(statuses))


def _pick(cur: Any, statuses: Sequence[str], rng: random.Random) -> tuple[int, str] | None:
    cur.execute(
        f"SELECT id, status FROM dbo.orders WHERE status IN ({_in(statuses)}) ORDER BY id",
        *statuses,
    )
    rows = [(int(r[0]), str(r[1])) for r in cur.fetchall()]
    return rng.choice(rows) if rows else None


def insert_order(cur: Any, rng: random.Random) -> int:
    cur.execute(
        "INSERT INTO dbo.orders (customer_id, status, amount, created_at, updated_at) "
        "OUTPUT INSERTED.id VALUES (?, ?, ?, SYSUTCDATETIME(), SYSUTCDATETIME())",
        rng.randint(1, 200),
        PENDING,
        random_amount(rng),
    )
    row = cur.fetchone()
    assert row is not None
    return int(row[0])


def update_order(cur: Any, rng: random.Random, cancel_probability: float) -> int | None:
    picked = _pick(cur, UPDATABLE, rng)
    if picked is None:
        return None
    order_id, status = picked
    if wants_amount_change(status, rng):
        cur.execute(
            "UPDATE dbo.orders SET amount = ?, updated_at = SYSUTCDATETIME() "
            "WHERE id = ? AND status = ?",
            random_amount(rng),
            order_id,
            PENDING,
        )
    else:
        new_status = next_status(status, rng, cancel_probability)
        cur.execute(
            "UPDATE dbo.orders SET status = ?, updated_at = SYSUTCDATETIME() "
            "WHERE id = ? AND status = ?",
            new_status,
            order_id,
            status,
        )
    return order_id


def delete_order(cur: Any, rng: random.Random) -> int | None:
    picked = _pick(cur, (DELIVERED, CANCELLED), rng)
    if picked is None:
        return None
    cur.execute(
        f"DELETE FROM dbo.orders WHERE id = ? AND status IN ({_in((DELIVERED, CANCELLED))})",
        picked[0],
        DELIVERED,
        CANCELLED,
    )
    return picked[0]


def apply_op(conn: Any, op: Op, rng: random.Random, cancel_probability: float) -> int | None:
    """One mutation in its own transaction; same contract as `mutator.db.apply_op`."""
    errors = transient_errors()
    cur = conn.cursor()
    try:
        if op == "c":
            result: int | None = insert_order(cur, rng)
        elif op == "u":
            result = update_order(cur, rng, cancel_probability)
        else:
            result = delete_order(cur, rng)
    except BaseException:
        with contextlib.suppress(*errors):
            conn.rollback()
        raise
    finally:
        with contextlib.suppress(*errors):
            cur.close()
    try:
        conn.commit()
    except errors as exc:
        raise CommitOutcomeUnknownError(f"connection lost while committing op {op!r}") from exc
    return result


def count_orders(conn: Any) -> int:
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM dbo.orders")
        row = cur.fetchone()
    finally:
        cur.close()
    assert row is not None
    return int(row[0])


def sqlserver_ops(settings: SqlServerSettings) -> DbOps:
    """The db-ops bundle the mutation loop runs on for `--target mssql`."""
    return DbOps(connect=lambda: connect(settings), apply_op=apply_op, transient=transient_errors())
