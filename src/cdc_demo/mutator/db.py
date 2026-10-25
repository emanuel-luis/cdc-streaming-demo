"""Postgres access for the mutator. Sync psycopg 3, parameterized SQL only.

Candidates are fetched ORDER BY id and picked in Python with the seeded RNG, so the same seed
over the same data makes the same changes.
"""

import contextlib
import random
from collections.abc import Sequence
from typing import Any

import psycopg

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

Cursor = psycopg.Cursor[Any]


def _pick(cur: Cursor, statuses: Sequence[str], rng: random.Random) -> tuple[int, str] | None:
    cur.execute(
        "SELECT id, status FROM orders WHERE status = ANY(%s) ORDER BY id", (list(statuses),)
    )
    rows = cur.fetchall()
    return rng.choice(rows) if rows else None


def insert_order(cur: Cursor, rng: random.Random) -> int:
    cur.execute(
        "INSERT INTO orders (customer_id, status, amount, created_at, updated_at) "
        "VALUES (%s, %s, %s, now(), now()) RETURNING id",
        (rng.randint(1, 200), PENDING, random_amount(rng)),
    )
    row = cur.fetchone()
    assert row is not None
    return int(row[0])


def update_order(cur: Cursor, rng: random.Random, cancel_probability: float) -> int | None:
    """Advance a non-terminal order, or change the amount of a pending one."""
    picked = _pick(cur, UPDATABLE, rng)
    if picked is None:
        return None
    order_id, status = picked
    if wants_amount_change(status, rng):
        cur.execute(
            "UPDATE orders SET amount = %s WHERE id = %s AND status = %s",
            (random_amount(rng), order_id, PENDING),
        )
    else:
        new_status = next_status(status, rng, cancel_probability)
        cur.execute(
            "UPDATE orders SET status = %s WHERE id = %s AND status = %s",
            (new_status, order_id, status),
        )
    return order_id


def delete_order(cur: Cursor, rng: random.Random) -> int | None:
    picked = _pick(cur, (DELIVERED, CANCELLED), rng)
    if picked is None:
        return None
    cur.execute(
        "DELETE FROM orders WHERE id = %s AND status = ANY(%s)",
        (picked[0], [DELIVERED, CANCELLED]),
    )
    return picked[0]


class CommitOutcomeUnknownError(Exception):
    """The connection failed during COMMIT: the change may or may not have been applied."""


def apply_op(
    conn: psycopg.Connection[Any], op: Op, rng: random.Random, cancel_probability: float
) -> int | None:
    """Run one mutation in its own transaction. Returns the order id, None when skipped.

    Expects a connection with autocommit off. An error before COMMIT leaves nothing applied
    (the server rolls back), so the caller may retry it; an error during COMMIT raises
    CommitOutcomeUnknownError, because retrying then could apply the change twice.
    """
    result: int | None
    try:
        with conn.cursor() as cur:
            if op == "c":
                result = insert_order(cur, rng)
            elif op == "u":
                result = update_order(cur, rng, cancel_probability)
            else:
                result = delete_order(cur, rng)
    except BaseException:
        # On a dead connection the rollback fails too; the server discards the transaction.
        with contextlib.suppress(psycopg.Error):
            conn.rollback()
        raise
    try:
        conn.commit()
    except psycopg.OperationalError as exc:
        raise CommitOutcomeUnknownError(f"connection lost while committing op {op!r}") from exc
    return result


def count_orders(conn: psycopg.Connection[Any]) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM orders")
        row = cur.fetchone()
    assert row is not None
    return int(row[0])
