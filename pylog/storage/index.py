"""Sparse offset -> byte-position mapping for one segment.

Not every record gets an entry — only every Nth one (or every N bytes).
lookup() returns the position of the largest indexed offset <= target;
the caller scans forward from there to find the exact record.
"""

from bisect import bisect_right

from pylog.storage.segment import Segment


class Index:
    def __init__(self, interval: int):
        """Start with an empty sparse index. `interval` is how many
        records to skip between stored entries — the index decides
        sparseness internally; callers just call `add` on every record."""
        self.interval = interval
        self._offsets: list[int] = []
        self._positions: list[int] = []
        self._since_last_entry = 0

    def add(self, offset: int, position: int) -> None:
        """Called on every record, in increasing offset order. Internally
        decides whether this particular (offset, position) is far enough
        past the last stored entry to actually be kept, per `interval`."""
        if not self._offsets or self._since_last_entry >= self.interval:
            self._offsets.append(offset)
            self._positions.append(position)
            self._since_last_entry = 0
        else:
            self._since_last_entry += 1

    def lookup(self, offset: int) -> int:
        """Return the byte position of the largest indexed offset <=
        `offset`. Raise if the index is empty or `offset` is below every
        indexed entry."""
        if not self._offsets:
            raise ValueError("index is empty")
        i = bisect_right(self._offsets, offset) - 1
        if i < 0:
            raise ValueError(f"offset {offset} is below every indexed entry")
        return self._positions[i]

    def rebuild_from(self, segment: Segment) -> None:
        """Reconstruct the index by scanning `segment` from the start,
        used after a crash when the index may be behind the log."""
        self._offsets = []
        self._positions = []
        self._since_last_entry = 0
        for position, record in segment.scan():
            self.add(record.offset, position)
