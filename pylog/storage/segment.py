"""A single file holding a contiguous range of records.

Filename convention: named after its base offset, zero-padded
(e.g. 00000000000000000000.log), so segment lookup is a sorted-filename
search.
"""

import os
import struct
from pathlib import Path
from typing import Iterator

from pylog.storage.record import HEADER_FORMAT, Record, decode, encode, header_size


class Segment:
    def __init__(self, path: Path, base_offset: int):
        """Open (or create) the segment file at `path`. `base_offset` is
        the offset of the first record this segment will ever hold."""
        self.path = path
        self.base_offset = base_offset
        self.file = open(path, "ab+")

    def append(self, record: Record) -> int:
        """Encode and write `record` to the end of the file. Return the
        byte position where this record's header starts, so an index can
        map offset -> position."""
        self.file.seek(0, 2)  # 2 = os.SEEK_END; jump to end-of-file
        position = self.file.tell()
        self.file.write(encode(record))
        self.file.flush()
        os.fsync(self.file.fileno())  # survive a crash right after this call
        return position

    def read_at(self, position: int) -> Record:
        """Read and decode exactly one record starting at byte `position`."""
        self.file.seek(position)
        header_bytes = self.file.read(header_size())
        _, _, key_length, value_length, _ = struct.unpack(HEADER_FORMAT, header_bytes)
        body = self.file.read(key_length + value_length)
        return decode(header_bytes + body)

    def read_from(self, position: int, max_bytes: int) -> list[Record]:
        """Sequentially decode as many whole records as fit within
        `max_bytes`, starting at byte `position`."""
        records = []
        self.file.seek(position)
        bytes_read = 0
        while bytes_read < max_bytes:
            header_bytes = self.file.read(header_size())
            if len(header_bytes) < header_size():
                break  # ran out of file before a full header
            _, _, key_length, value_length, _ = struct.unpack(
                HEADER_FORMAT, header_bytes
            )
            body = self.file.read(key_length + value_length)
            if len(body) < key_length + value_length:
                break  # torn record at end of file
            records.append(decode(header_bytes + body))
            bytes_read = self.file.tell() - position
        return records

    def size(self) -> int:
        """Current byte size of the segment file, used for roll decisions."""
        return self.path.stat().st_size

    def scan(self) -> Iterator[tuple[int, Record]]:
        """Yield (position, record) for every whole, checksum-valid record
        from the start of the file. Stops silently — without raising — at
        the first short read or CRC mismatch, since that's exactly what a
        torn write (crash mid-append) looks like on disk. Used to rebuild
        the index and to find where a torn tail begins."""
        self.file.seek(0)
        position = 0
        size = self.size()
        while position < size:
            header_bytes = self.file.read(header_size())
            if len(header_bytes) < header_size():
                break  # torn header at end of file
            _, _, key_length, value_length, _ = struct.unpack(
                HEADER_FORMAT, header_bytes
            )
            body = self.file.read(key_length + value_length)
            if len(body) < key_length + value_length:
                break  # torn body at end of file
            try:
                record = decode(header_bytes + body)
            except ValueError:
                break  # header parsed but checksum is wrong: corrupt/torn
            yield position, record
            position = self.file.tell()

    def truncate(self, position: int) -> None:
        """Discard everything in the file after `position`. Used during
        recovery to cut off a torn tail left by a crash mid-append, so the
        segment only ever contains whole, checksum-valid records."""
        self.file.truncate(position)
        self.file.flush()
        os.fsync(self.file.fileno())
