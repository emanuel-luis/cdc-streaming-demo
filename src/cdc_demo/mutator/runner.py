"""Mutation loop: rate limiting, retry with backoff, manifest."""

import logging
import random
import secrets
import time
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg

from cdc_demo.logging import log_event
from cdc_demo.mutator.db import apply_op
from cdc_demo.mutator.model import Counts, Manifest, Op, choose_op
from cdc_demo.settings import MutatorSettings, PostgresSettings

log = logging.getLogger(__name__)

MAX_RETRIES = 6
BACKOFF_BASE_S = 0.5


def write_manifest(manifest: Manifest, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(manifest.model_dump_json(indent=2) + "\n", encoding="utf-8")


def run_mutations(
    pg: PostgresSettings,
    settings: MutatorSettings,
    *,
    count: int | None = None,
    duration: float | None = None,
    rate: float | None = None,
    seed: int | None = None,
    connect: Callable[..., psycopg.Connection[Any]] = psycopg.connect,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> Manifest:
    if (count is None) == (duration is None):
        raise ValueError("pass exactly one of count or duration")
    rate = rate if rate is not None else settings.rate
    if rate <= 0:
        raise ValueError("rate must be positive")
    seed = seed if seed is not None else settings.seed
    if seed is None:
        seed = secrets.randbelow(2**31)
    rng = random.Random(seed)
    weights: dict[Op, float] = {
        "c": settings.insert_weight,
        "u": settings.update_weight,
        "d": settings.delete_weight,
    }
    run_id = uuid.uuid4().hex
    started_at = datetime.now(UTC)
    counts = {"c": 0, "u": 0, "d": 0}
    skipped = 0
    conn: psycopg.Connection[Any] | None = None

    def mutate(op: Op, index: int) -> int | None:
        # One reconnect-and-retry loop per mutation. apply_op raises OperationalError only
        # before COMMIT, when the server has rolled back, so a retry never double-applies; a
        # failure during COMMIT (CommitOutcomeUnknownError) is not retried and ends the run.
        # Each attempt gets a fresh RNG seeded by (seed, index): a retry makes the same choices
        # as the failed attempt, so a run with retries matches a clean run with the same seed.
        nonlocal conn
        for attempt in range(MAX_RETRIES + 1):
            try:
                if conn is None:
                    conn = connect(**pg.connect_kwargs(), autocommit=False)
                op_rng = random.Random(f"{seed}:{index}")
                return apply_op(conn, op, op_rng, settings.cancel_probability)
            except psycopg.OperationalError as exc:
                if conn is not None:
                    conn.close()
                    conn = None
                if attempt == MAX_RETRIES:
                    raise
                delay = BACKOFF_BASE_S * 2**attempt
                log_event(
                    log,
                    "retry",
                    run_id=run_id,
                    attempt=attempt + 1,
                    delay_s=delay,
                    error=type(exc).__name__,
                )
                sleep(delay)
        raise AssertionError("unreachable")

    start = clock()
    done = 0
    try:
        while True:
            if count is not None and done >= count:
                break
            if duration is not None and clock() - start >= duration:
                break
            op = choose_op(rng, weights)
            order_id = mutate(op, done)
            if order_id is None:
                skipped += 1
            else:
                counts[op] += 1
            log_event(
                log, "mutation", run_id=run_id, op=op, order_id=order_id, skipped=order_id is None
            )
            done += 1
            wait = start + done / rate - clock()
            if wait > 0:
                sleep(wait)
    finally:
        if conn is not None:
            conn.close()

    return Manifest(
        run_id=run_id,
        started_at=started_at,
        finished_at=datetime.now(UTC),
        seed=seed,
        requested=count,
        counts=Counts(**counts),
        skipped=skipped,
    )
