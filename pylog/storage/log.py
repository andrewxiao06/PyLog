"""One partition: a sequence of segments, treated as a single continuous
append-only stream of offsets. Handles segment rolling and, on startup,
crash recovery — rebuilding indexes and cutting off any torn tail."""

import time
from pathlib import Path

from pylog.storage.index import Index
from pylog.storage.record import Record, header_size
from pylog.storage.segment import Segment

SEGMENT_FILENAME_WIDTH = 20
DEFAULT_ROLL_SIZE = 4 * 1024 * 1024  # 1-4MB per CLAUDE.md, so rolling is observable
DEFAULT_INDEX_INTERVAL = 16


def _segment_filename(base_offset: int) -> str:
    return f"{base_offset:0{SEGMENT_FILENAME_WIDTH}d}.log"


def _record_size_on_disk(record: Record) -> int:
    key_len = len(record.key) if record.key is not None else 0
    return header_size() + key_len + len(record.value)


class Log:
    def __init__(
        self,
        directory: Path,
        roll_size: int = DEFAULT_ROLL_SIZE,
        index_interval: int = DEFAULT_INDEX_INTERVAL,
    ):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.roll_size = roll_size
        self.index_interval = index_interval

        self._segments: list[Segment] = []
        self._indexes: dict[int, Index] = {}
        self._next_offset = 0

        self._open_segments()
        self.recover()

    def _open_segments(self) -> None:
        existing = sorted(self.directory.glob("*.log"))
        if not existing:
            existing = [self.directory / _segment_filename(0)]
        for path in existing:
            base_offset = int(path.stem)
            self._segments.append(Segment(path, base_offset))
            self._indexes[base_offset] = Index(self.index_interval)

    @property
    def active_segment(self) -> Segment:
        return self._segments[-1]

    @property
    def high_watermark(self) -> int:
        """Offset one past the last record ever appended. Equivalently:
        the offset the next append() call will assign."""
        return self._next_offset

    def recover(self) -> None:
        """Rebuild every segment's sparse index from what's actually on
        disk, and truncate any torn tail left by a crash mid-append.

        Order matters: a crash can land between `Segment.append`
        returning (bytes on disk) and the caller recording that offset
        in the index, so the index can never be trusted after an unclean
        shutdown — only the segment bytes themselves, validated by CRC,
        are the source of truth. We rebuild the index by re-deriving it
        from those validated bytes rather than trusting whatever the
        index file last had.
        """
        highest_offset = -1
        for segment in self._segments:
            index = self._indexes[segment.base_offset]
            last_good_end = 0
            last_offset = None
            for position, record in segment.scan():
                index.add(record.offset, position)
                last_good_end = position + _record_size_on_disk(record)
                last_offset = record.offset

            if segment.size() != last_good_end:
                # Bytes past last_good_end are a torn record: the process
                # died after write() returned some bytes to the OS but
                # before the full record made it out, or before fsync.
                segment.truncate(last_good_end)

            if last_offset is not None:
                highest_offset = max(highest_offset, last_offset)

        self._next_offset = highest_offset + 1

    def _roll(self) -> None:
        new_base = self._next_offset
        segment = Segment(self.directory / _segment_filename(new_base), new_base)
        self._segments.append(segment)
        self._indexes[new_base] = Index(self.index_interval)

    def append(self, key: bytes | None, value: bytes) -> int:
        if self.active_segment.size() >= self.roll_size:
            self._roll()

        offset = self._next_offset
        record = Record(offset=offset, timestamp=time.time_ns(), key=key, value=value)
        position = self.active_segment.append(record)
        self._indexes[self.active_segment.base_offset].add(offset, position)
        self._next_offset += 1
        return offset

    def _segment_for_offset(self, offset: int) -> Segment:
        for segment in reversed(self._segments):
            if offset >= segment.base_offset:
                return segment
        raise ValueError(f"offset {offset} predates every segment")

    def read_at(self, offset: int) -> Record:
        if offset < 0 or offset >= self._next_offset:
            raise ValueError(
                f"offset {offset} out of range (high watermark {self._next_offset})"
            )
        segment = self._segment_for_offset(offset)
        index = self._indexes[segment.base_offset]
        position = index.lookup(offset)
        # index is sparse: scan forward from the nearest indexed entry
        while True:
            record = segment.read_at(position)
            if record.offset == offset:
                return record
            position += _record_size_on_disk(record)

    def read_from(self, offset: int, max_bytes: int) -> list[Record]:
        """Read up to max_bytes worth of records starting at `offset`,
        possibly spanning into later segments."""
        if offset >= self._next_offset:
            return []

        records: list[Record] = []

        # Find the starting segment, then walk forward across segment
        # boundaries until max_bytes is exhausted or we hit the high watermark.
        start_index = 0
        for i, segment in enumerate(self._segments):
            if segment.base_offset <= offset:
                start_index = i

        remaining = max_bytes
        current_offset = offset
        for segment in self._segments[start_index:]:
            if remaining <= 0:
                break
            index = self._indexes[segment.base_offset]
            try:
                position = index.lookup(max(current_offset, segment.base_offset))
            except ValueError:
                position = 0
            batch = segment.read_from(position, remaining)
            for record in batch:
                if record.offset < current_offset:
                    continue
                records.append(record)
                remaining -= _record_size_on_disk(record)
                if remaining <= 0:
                    break
            current_offset = segment.base_offset + len(batch)

        return records
