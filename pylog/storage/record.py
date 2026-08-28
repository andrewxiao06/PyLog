"""Wire format encode/decode for a single record.

Layout (per CLAUDE.md design decisions):

Fixed header — same size every record:
    offset        (8 bytes) — broker-assigned, monotonically increasing per partition
    timestamp     (8 bytes) — when the broker received the record
    key_length    (4 bytes) — length of the key in bytes (0 if key is None)
    value_length  (4 bytes) — length of the value in bytes
    crc           (4 bytes) — checksum over key + value, for torn-write detection

Variable body — length given by the header above:
    key           (key_length bytes)
    value         (value_length bytes)
"""

import struct
from dataclasses import dataclass
from zlib import crc32

# little-endian: offset(Q), timestamp(Q), key_length(I), value_length(I), crc(I)
HEADER_FORMAT = "<QQIII"


@dataclass
class Record:
    offset: int
    timestamp: int
    key: bytes | None
    value: bytes


def encode(record: Record) -> bytes:
    """Pack a Record into its on-disk byte representation: fixed header
    followed by key bytes followed by value bytes."""
    key_bytes = record.key if record.key is not None else b""
    checksum = crc32(key_bytes + record.value)
    header = struct.pack(
        HEADER_FORMAT,
        record.offset,
        record.timestamp,
        len(key_bytes),
        len(record.value),
        checksum,
    )
    return header + key_bytes + record.value


def decode(data: bytes) -> Record:
    """Unpack a single record from a bytes buffer that starts exactly at
    the record's header. Must validate the checksum and raise if it
    doesn't match."""
    header = data[: header_size()]
    offset, timestamp, key_length, value_length, checksum = struct.unpack(
        HEADER_FORMAT, header
    )

    key_start = header_size()
    key_end = key_start + key_length
    value_end = key_end + value_length

    key_bytes = data[key_start:key_end]
    value_bytes = data[key_end:value_end]

    if crc32(key_bytes + value_bytes) != checksum:
        raise ValueError("checksum mismatch: record is corrupt or torn")

    return Record(
        offset=offset,
        timestamp=timestamp,
        key=key_bytes if key_length > 0 else None,
        value=value_bytes,
    )


def header_size() -> int:
    """Return the fixed number of bytes every record's header occupies,
    so callers know how many bytes to read before they can determine the
    variable body's length."""
    return struct.calcsize(HEADER_FORMAT)
