"""In-memory counters and latency samples, exposed in Prometheus's plain
text exposition format so a Prometheus server can scrape them over HTTP.
These are real numbers from whatever traffic has hit this process since
it started — not the sustained-load benchmark numbers the resume-bullets
section calls for, which still need an actual load generator and the
crash-injection harness (Week 3-4 in CLAUDE.md) run against this."""


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

    def render_prometheus(self) -> str:
        """Render as Prometheus's text exposition format: one `# HELP`
        and `# TYPE` line per metric, then `name value` pairs. This is
        the exact format `GET /metrics` has to return for a Prometheus
        server to scrape it — no client library needed for something
        this small."""
        p50 = self.percentile(50)
        p99 = self.percentile(99)
        lines = [
            "# HELP pylog_produce_total Total PRODUCE requests handled.",
            "# TYPE pylog_produce_total counter",
            f"pylog_produce_total {self.produce_count}",
            "# HELP pylog_fetch_total Total FETCH requests handled.",
            "# TYPE pylog_fetch_total counter",
            f"pylog_fetch_total {self.fetch_count}",
            "# HELP pylog_produce_latency_ms_p50 Median PRODUCE latency in milliseconds.",
            "# TYPE pylog_produce_latency_ms_p50 gauge",
            f"pylog_produce_latency_ms_p50 {p50 if p50 is not None else 0}",
            "# HELP pylog_produce_latency_ms_p99 p99 PRODUCE latency in milliseconds.",
            "# TYPE pylog_produce_latency_ms_p99 gauge",
            f"pylog_produce_latency_ms_p99 {p99 if p99 is not None else 0}",
        ]
        return "\n".join(lines) + "\n"
