"""Pure mutator model: status machine, operation choice and the run manifest. No I/O."""

import random
from decimal import Decimal
from typing import Final, Literal

from pydantic import AwareDatetime, BaseModel, Field

Op = Literal["c", "u", "d"]
OPS: Final[tuple[Op, ...]] = ("c", "u", "d")

PENDING, CONFIRMED, SHIPPED, DELIVERED, CANCELLED = (
    "pending",
    "confirmed",
    "shipped",
    "delivered",
    "cancelled",
)
STATUSES: Final = (PENDING, CONFIRMED, SHIPPED, DELIVERED, CANCELLED)
TERMINAL: Final = frozenset({DELIVERED, CANCELLED})
UPDATABLE: Final = (PENDING, CONFIRMED, SHIPPED)
# Only a pending order may change its amount; the forward move is still the common case.
AMOUNT_CHANGE_PROBABILITY: Final = 0.25

_FORWARD: Final = {PENDING: CONFIRMED, CONFIRMED: SHIPPED, SHIPPED: DELIVERED}
_CANCELLABLE: Final = frozenset({PENDING, CONFIRMED})


def next_status(status: str, rng: random.Random, cancel_probability: float) -> str | None:
    """Next status, or None when the order is terminal."""
    forward = _FORWARD.get(status)
    if forward is None:
        return None
    if status in _CANCELLABLE and rng.random() < cancel_probability:
        return CANCELLED
    return forward


def wants_amount_change(status: str, rng: random.Random) -> bool:
    return status == PENDING and rng.random() < AMOUNT_CHANGE_PROBABILITY


def choose_op(rng: random.Random, weights: dict[Op, float]) -> Op:
    """Weighted choice; an operation with weight zero is never returned."""
    ops = [op for op in OPS if weights.get(op, 0) > 0]
    if not ops:
        raise ValueError("at least one operation weight must be positive")
    return rng.choices(ops, weights=[weights[op] for op in ops])[0]


def random_amount(rng: random.Random) -> Decimal:
    return Decimal(rng.randrange(100, 100_000)).scaleb(-2)


class Counts(BaseModel):
    c: int = 0
    u: int = 0
    d: int = 0


class Manifest(BaseModel):
    """What a mutator run did; `verify` compares the lake against it."""

    run_id: str
    started_at: AwareDatetime
    finished_at: AwareDatetime
    seed: int
    requested: int | None
    counts: Counts = Field(default_factory=Counts)
    skipped: int = 0
