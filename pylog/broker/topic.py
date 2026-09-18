"""A Topic is N partitions plus the routing rule that picks one for a
given key."""

import itertools
from pathlib import Path
from zlib import crc32

from pylog.broker.partition import Partition


class Topic:
    def __init__(self, name: str, partition_count: int, directory: Path, **log_kwargs):
        self.name = name
        self.partition_count = partition_count
        self.partitions = [
            Partition(name, i, directory / f"partition-{i}", **log_kwargs)
            for i in range(partition_count)
        ]
        self._round_robin = itertools.cycle(range(partition_count))

    def partition_for_key(self, key: bytes | None) -> int:
        """Deterministic routing: the same key always lands on the same
        partition, including across process restarts. Uses crc32 rather
        than Python's built-in hash() — hash() is salted per-process via
        PYTHONHASHSEED, so the same key would route to a different
        partition every time the broker restarts."""
        if key is None:
            return next(self._round_robin)
        return crc32(key) % self.partition_count

    def append(self, key: bytes | None, value: bytes) -> tuple[int, int]:
        partition_id = self.partition_for_key(key)
        offset = self.partitions[partition_id].append(key, value)
        return partition_id, offset

    def read_from(self, partition_id: int, offset: int, max_bytes: int):
        return self.partitions[partition_id].read_from(offset, max_bytes)
