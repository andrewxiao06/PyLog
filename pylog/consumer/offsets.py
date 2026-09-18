"""Durable committed-offset storage: one file per (group, topic,
partition). Chosen over an internal Kafka-style commit topic for
simplicity — no consumer needed to read your own commits back, no
compaction to reclaim old commit records, at the cost of not getting
commits "for free" as regular appended records.

Each commit is written atomically via write-to-temp + fsync + rename,
so a crash mid-commit either leaves the old offset intact or the new
one fully written — never a torn/partial offset value. Combined with
"commit after processing, not before," this is what makes at-least-once
delivery hold: a crash before a commit lands means the consumer re-reads
records it already processed, but a crash never causes it to skip past
work it hasn't done."""

import os
from pathlib import Path


class OffsetStore:
    def __init__(self, directory: Path):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)

    def _path(self, group: str, topic: str, partition: int) -> Path:
        return self.directory / f"{group}__{topic}__{partition}.offset"

    def commit(self, group: str, topic: str, partition: int, offset: int) -> None:
        path = self._path(group, topic, partition)
        tmp_path = path.with_suffix(".tmp")
        with open(tmp_path, "w") as f:
            f.write(str(offset))
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)  # atomic on POSIX: no partially-written commit is ever visible

    def fetch(self, group: str, topic: str, partition: int) -> int:
        """Last committed offset for this group/topic/partition, or 0 if
        this group has never committed here (start from the beginning)."""
        path = self._path(group, topic, partition)
        if not path.exists():
            return 0
        return int(path.read_text())
