"""In-memory counters and latency samples. Enough to point at for "where's
my throughput ceiling" once paired with an actual load generator and the
crash-injection harness (Week 3-4 in CLAUDE.md) — not a Prometheus
exporter, and these are not the numbers referenced in the resume-bullets
section, which stay bracketed until that benchmarking actually happens."""


class Metrics:
    def __init__(self):
        self.produce_count = 0
        self.fetch_count = 0
        self.produce_latencies_ms: list[float] = []

    def record_produce(self, latency_ms: float) -> None:
        self.produce_count += 1
        self.produce_latencies_ms.append(latency_ms)

    def record_fetch(self) -> None:
        self.fetch_count += 1

    def percentile(self, p: float) -> float | None:
        if not self.produce_latencies_ms:
            return None
        data = sorted(self.produce_latencies_ms)
        index = min(len(data) - 1, int(len(data) * p / 100))
        return data[index]

    def snapshot(self) -> dict:
        return {
            "produce_count": self.produce_count,
            "fetch_count": self.fetch_count,
            "produce_latency_p50_ms": self.percentile(50),
            "produce_latency_p99_ms": self.percentile(99),
        }
