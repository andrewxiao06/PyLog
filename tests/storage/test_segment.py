from pathlib import Path

from pylog.storage.record import Record
from pylog.storage.segment import Segment


def test_append_then_read_at_returns_same_record(tmp_path: Path):
    segment = Segment(tmp_path / "segment.log", 0)
    record = Record(0, 67, b"key1", b"test1")

    position = segment.append(record)
    result = segment.read_at(position)

    assert result == record


def test_read_from_returns_all_records_in_order(tmp_path: Path):
    segment = Segment(tmp_path / "segment.log", 0)
    records = [
        Record(0, 1, b"key1", b"value1"),
        Record(1, 2, b"key2", b"value2"),
        Record(2, 3, None, b"value3"),
    ]

    for record in records:
        segment.append(record)

    result = segment.read_from(0, segment.size())

    assert result == records
