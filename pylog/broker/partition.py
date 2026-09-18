"""A Partition pairs one Log with its topic/partition identity."""

from pathlib import Path

from pylog.storage.log import Log
from pylog.storage.record import Record


class Partition:
    def __init__(self, topic: str, partition_id: int, directory: Path, **log_kwargs):
        self.topic = topic
        self.partition_id = partition_id
        self.log = Log(directory, **log_kwargs)

    def append(self, key: bytes | None, value: bytes) -> int:
        return self.log.append(key, value)

    def read_from(self, offset: int, max_bytes: int) -> list[Record]:
        return self.log.read_from(offset, max_bytes)

    @property
    def high_watermark(self) -> int:
        return self.log.high_watermark
