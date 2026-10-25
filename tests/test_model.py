import random
from collections import Counter

from hypothesis import given
from hypothesis import strategies as st

from cdc_demo.mutator.model import (
    STATUSES,
    TERMINAL,
    choose_op,
    next_status,
)

ORDER = {s: i for i, s in enumerate(["pending", "confirmed", "shipped", "delivered"])}
probabilities = st.floats(min_value=0, max_value=1)


@given(st.sampled_from(STATUSES), st.integers(), probabilities)
def test_transitions_are_forward_only(status: str, seed: int, p: float) -> None:
    new = next_status(status, random.Random(seed), p)
    if status in TERMINAL:
        assert new is None
    else:
        assert new is not None
        assert new == "cancelled" or ORDER[new] == ORDER[status] + 1
        assert new != "cancelled" or status in {"pending", "confirmed"}


@given(st.integers(), probabilities)
def test_walk_always_terminates(seed: int, p: float) -> None:
    rng, status = random.Random(seed), "pending"
    for _ in range(4):
        new = next_status(status, rng, p)
        if new is None:
            break
        status = new
    assert status in TERMINAL


def test_cancel_probability_extremes() -> None:
    rng = random.Random(0)
    assert next_status("pending", rng, 1.0) == "cancelled"
    assert next_status("pending", rng, 0.0) == "confirmed"
    assert next_status("shipped", rng, 1.0) == "delivered"


weights = st.lists(st.floats(min_value=0, max_value=10), min_size=3, max_size=3).filter(
    lambda w: sum(w) > 0
)


@given(weights, st.integers())
def test_zero_weight_ops_never_chosen(w: list[float], seed: int) -> None:
    mapping = {"c": w[0], "u": w[1], "d": w[2]}
    rng = random.Random(seed)
    chosen = Counter(choose_op(rng, mapping) for _ in range(200))  # type: ignore[arg-type]
    for op, weight in mapping.items():
        if weight == 0:
            assert chosen[op] == 0


@given(weights, st.integers())
def test_same_seed_same_sequence(w: list[float], seed: int) -> None:
    mapping = {"c": w[0], "u": w[1], "d": w[2]}

    def run() -> list[str]:
        rng = random.Random(seed)
        return [choose_op(rng, mapping) for _ in range(50)]  # type: ignore[arg-type]

    assert run() == run()
