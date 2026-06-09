from __future__ import annotations

import asyncio
import json
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass
class PipelineEvent:
    document_id: str
    module: str
    status: str
    data: dict
    errors: list[str]
    duration_ms: float
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def to_sse(self) -> str:
        payload = json.dumps(
            {
                "document_id": self.document_id,
                "module": self.module,
                "status": self.status,
                "data": self.data,
                "errors": self.errors,
                "duration_ms": round(self.duration_ms, 2),
                "timestamp": self.timestamp,
            }
        )
        return f"data: {payload}\n\n"


def _percentile(sorted_vals: list[float], p: float) -> float:
    """Compute percentile p (0–100) from a pre-sorted list."""
    if not sorted_vals:
        return 0.0
    idx = (len(sorted_vals) - 1) * p / 100.0
    lo = int(idx)
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = idx - lo
    return round(sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac, 2)


class EventStore:
    def __init__(self, maxlen: int = 2000) -> None:
        self._events: deque[PipelineEvent] = deque(maxlen=maxlen)
        self._queues: list[asyncio.Queue[PipelineEvent]] = []
        self._totals: dict[str, int] = {"total": 0, "success": 0, "error": 0, "skipped": 0}
        self._duration_sum: float = 0.0
        # All durations kept for percentile computation (bounded to last maxlen)
        self._all_durations: deque[float] = deque(maxlen=maxlen)
        self._by_module: dict[str, dict[str, Any]] = {}
        self._by_route: dict[str, int] = {}
        self._by_doc_type: dict[str, int] = {}
        self._llm_calls: int = 0
        # Recent errors fast-access buffer (last 50 error events)
        self._recent_errors: deque[PipelineEvent] = deque(maxlen=50)
        # Hourly throughput: ring buffer of (wall_clock_time,) tuples for events in last 60 min
        self._event_times: deque[float] = deque(maxlen=maxlen)
        # Quality score tracking (from generator module)
        self._quality_scores: deque[int] = deque(maxlen=maxlen)
        # Per-module duration deques for per-module percentiles (last 500)
        self._module_durations: dict[str, deque[float]] = {}

    async def record(self, event: PipelineEvent) -> None:
        self._events.append(event)
        self._update_stats(event)
        await self._broadcast(event)

    def _update_stats(self, event: PipelineEvent) -> None:
        now = time.time()
        self._event_times.append(now)

        self._totals["total"] += 1
        self._totals[event.status] = self._totals.get(event.status, 0) + 1
        self._duration_sum += event.duration_ms
        self._all_durations.append(event.duration_ms)

        if event.status == "error":
            self._recent_errors.append(event)

        m = self._by_module.setdefault(
            event.module,
            {"total": 0, "success": 0, "error": 0, "skipped": 0, "duration_sum": 0.0},
        )
        m["total"] += 1
        m[event.status] = m.get(event.status, 0) + 1
        m["duration_sum"] += event.duration_ms

        # Per-module duration deque for percentiles
        mod_durs = self._module_durations.setdefault(event.module, deque(maxlen=500))
        mod_durs.append(event.duration_ms)

        route = event.data.get("route")
        if route:
            self._by_route[route] = self._by_route.get(route, 0) + 1

        if event.module == "classifier":
            doc_type = event.data.get("type")
            if doc_type:
                self._by_doc_type[doc_type] = self._by_doc_type.get(doc_type, 0) + 1
            if event.data.get("method") == "llm":
                self._llm_calls += 1

        if event.module == "extractor" and event.data.get("llm_enriched"):
            self._llm_calls += 1

        if event.module == "generator":
            output = event.data.get("output", {})
            qs = output.get("quality_score")
            if isinstance(qs, int):
                self._quality_scores.append(qs)

    async def _broadcast(self, event: PipelineEvent) -> None:
        dead: list[asyncio.Queue] = []
        for q in self._queues:
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                dead.append(q)
        for q in dead:
            if q in self._queues:
                self._queues.remove(q)

    def subscribe(self) -> asyncio.Queue[PipelineEvent]:
        q: asyncio.Queue[PipelineEvent] = asyncio.Queue(maxsize=200)
        self._queues.append(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        if q in self._queues:
            self._queues.remove(q)

    def recent(self, n: int = 100) -> list[PipelineEvent]:
        return list(self._events)[-n:]

    def recent_errors(self, n: int = 20) -> list[PipelineEvent]:
        """Return the last n error events."""
        return list(self._recent_errors)[-n:]

    def throughput(self, window_seconds: int = 3600) -> int:
        """Count events recorded within the last window_seconds."""
        cutoff = time.time() - window_seconds
        # Scan from the right (most recent) until we fall outside the window
        count = 0
        for t in reversed(self._event_times):
            if t >= cutoff:
                count += 1
            else:
                break
        return count

    def _duration_percentiles(self, durations: deque[float]) -> dict[str, float]:
        """Return p50/p95/p99 latency percentiles for a deque of durations."""
        if not durations:
            return {"p50": 0.0, "p95": 0.0, "p99": 0.0}
        sorted_vals = sorted(durations)
        return {
            "p50": _percentile(sorted_vals, 50),
            "p95": _percentile(sorted_vals, 95),
            "p99": _percentile(sorted_vals, 99),
        }

    @property
    def stats(self) -> dict:
        total = self._totals["total"]
        success = self._totals.get("success", 0)
        errors = self._totals.get("error", 0)

        # Quality score aggregates
        qs_list = list(self._quality_scores)
        avg_quality = round(sum(qs_list) / len(qs_list), 1) if qs_list else None

        return {
            "total": total,
            "success": success,
            "error": errors,
            "skipped": self._totals.get("skipped", 0),
            "success_rate": round(success / total * 100, 1) if total else 0.0,
            "avg_duration_ms": round(self._duration_sum / total, 2) if total else 0.0,
            "latency_percentiles": self._duration_percentiles(self._all_durations),
            "llm_calls": self._llm_calls,
            "throughput": {
                "last_1m":  self.throughput(60),
                "last_5m":  self.throughput(300),
                "last_60m": self.throughput(3600),
            },
            "avg_quality_score": avg_quality,
            "by_route": dict(sorted(self._by_route.items(), key=lambda x: -x[1])),
            "by_doc_type": dict(sorted(self._by_doc_type.items(), key=lambda x: -x[1])),
            "by_module": {
                name: {
                    "total": s["total"],
                    "success": s.get("success", 0),
                    "error": s.get("error", 0),
                    "avg_ms": round(s["duration_sum"] / s["total"], 2) if s["total"] else 0.0,
                    "latency": self._duration_percentiles(
                        self._module_durations.get(name, deque())
                    ),
                }
                for name, s in self._by_module.items()
            },
        }


store = EventStore()
