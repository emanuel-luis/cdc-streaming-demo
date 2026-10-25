"""Shared test helpers."""

import json
from collections.abc import Callable
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def fixture_text() -> Callable[[str], str]:
    def load(name: str) -> str:
        # Re-serialize to one line, as Debezium sends it.
        return json.dumps(json.loads((FIXTURES / f"{name}.json").read_text(encoding="utf-8")))

    return load
