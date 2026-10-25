import pytest
from hypothesis import given
from hypothesis import strategies as st

from cdc_demo.mssql.lsn import (
    BLOCK_LIMIT,
    SLOT_LIMIT,
    VLF_LIMIT,
    LsnOutOfRangeError,
    pack_lsn,
    parse_lsn,
    unpack_lsn,
)

vlfs = st.integers(0, VLF_LIMIT - 1)
blocks = st.integers(0, BLOCK_LIMIT - 1)
slots = st.integers(0, SLOT_LIMIT - 1)


def text(vlf: int, block: int, slot: int) -> str:
    return f"0x{vlf:08X}{block:08X}{slot:04X}"


@given(vlfs, blocks, slots)
def test_round_trip(vlf: int, block: int, slot: int) -> None:
    packed = pack_lsn(text(vlf, block, slot))
    assert 0 <= packed < 2**63
    assert unpack_lsn(packed) == text(vlf, block, slot)


@given(st.tuples(vlfs, blocks, slots), st.tuples(vlfs, blocks, slots))
def test_integer_order_is_lsn_order(a: tuple[int, int, int], b: tuple[int, int, int]) -> None:
    assert (pack_lsn(text(*a)) < pack_lsn(text(*b))) == (a < b)


@pytest.mark.parametrize("vlf, block", [(VLF_LIMIT, 0), (0, BLOCK_LIMIT)])
def test_out_of_range_is_rejected(vlf: int, block: int) -> None:
    with pytest.raises(LsnOutOfRangeError):
        pack_lsn(text(vlf, block, 0))


def test_largest_value_fits() -> None:
    assert pack_lsn(text(VLF_LIMIT - 1, BLOCK_LIMIT - 1, SLOT_LIMIT - 1)) == 2**63 - 1


@pytest.mark.parametrize("bad", ["", "0x12", "0x" + "G" * 20, "1" * 20, "0x" + "0" * 22])
def test_malformed_text(bad: str) -> None:
    with pytest.raises(ValueError):
        parse_lsn(bad)


def test_negative_value() -> None:
    with pytest.raises(ValueError):
        unpack_lsn(-1)
