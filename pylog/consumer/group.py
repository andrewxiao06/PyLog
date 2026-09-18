"""Consumer group membership and partition assignment.

Assignment rule: partition p is owned by members[p % len(members)].
Any join/leave reassigns everyone from scratch (no incremental/sticky
rebalancing) — simplest thing that still guarantees one partition maps
to exactly one consumer at a time."""

from dataclasses import dataclass, field


@dataclass
class ConsumerGroup:
    group: str
    topic: str
    partition_count: int
    members: list[str] = field(default_factory=list)

    def join(self, consumer_id: str) -> list[int]:
        if consumer_id not in self.members:
            self.members.append(consumer_id)
        return self.assignment_for(consumer_id)

    def leave(self, consumer_id: str) -> None:
        if consumer_id in self.members:
            self.members.remove(consumer_id)

    def assignment_for(self, consumer_id: str) -> list[int]:
        if consumer_id not in self.members:
            return []
        index = self.members.index(consumer_id)
        member_count = len(self.members)
        return [p for p in range(self.partition_count) if p % member_count == index]
