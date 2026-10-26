"""SQL Server LSN text <-> the BIGINT `source_lsn` of the CDC log.

An LSN is 10 bytes: virtual log file sequence (4), log block offset (4), slot (2); the library
carries it as `0x` + 20 uppercase hex digits. The BIGINT is `vlf << 40 | block << 16 | slot`,
so integer order is LSN order. It only fits when `vlf < 2**23` and `block < 2**24`; a larger
value is rejected (`lsn_out_of_range`), never truncated. SQL Server LSNs are not comparable
with Postgres ones: the two lakes stay apart (docs/DECISIONS.md, 45).
"""

import re

VLF_LIMIT = 1 << 23
BLOCK_LIMIT = 1 << 24
SLOT_LIMIT = 1 << 16

_LSN = re.compile(r"0x([0-9A-Fa-f]{8})([0-9A-Fa-f]{8})([0-9A-Fa-f]{4})")


class LsnOutOfRangeError(ValueError):
    """The LSN does not fit the BIGINT layout."""


def parse_lsn(text: str) -> tuple[int, int, int]:
    """(vlf, block, slot) of an LSN text; ValueError when it is not `0x` + 20 hex digits."""
    match = _LSN.fullmatch(text.strip())
    if match is None:
        raise ValueError(f"not an LSN: {text!r}")
    vlf, block, slot = (int(g, 16) for g in match.groups())
    return vlf, block, slot


def pack_lsn(text: str) -> int:
    vlf, block, slot = parse_lsn(text)
    if vlf >= VLF_LIMIT or block >= BLOCK_LIMIT:
        raise LsnOutOfRangeError(f"LSN {text} does not fit 63 bits")
    return vlf << 40 | block << 16 | slot


def unpack_lsn(value: int) -> str:
    if value < 0:
        raise ValueError(f"negative source_lsn: {value}")
    vlf, block, slot = value >> 40, value >> 16 & (BLOCK_LIMIT - 1), value & (SLOT_LIMIT - 1)
    return f"0x{vlf:08X}{block:08X}{slot:04X}"
