from __future__ import annotations

import asyncio
import json
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone


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


class EventStore:
    def __init__(self, maxlen: int = 2000) -> None:
        self._events: deque[PipelineEvent] = deque(maxlen=maxlen)
        self._queues: list[asyncio.Queue[PipelineEvent]] = []
        self._totals: dict[str, int] = {"total": 0, "success": 0, "error": 0, "skipped": 0}
        self._duration_sum: float = 0.0
        self._by_module: dict[str, dict] = {}
        self._by_route: dict[str, int] = {}

    async def record(self, event: PipelineEvent) -> None:
        self._events.append(event)
        self._update_stats(event)
        await self._broadcast(event)

    def _update_stats(self, event: PipelineEvent) -> None:
        self._totals["total"] += 1
        self._totals[event.status] = self._totals.get(event.status, 0) + 1
        self._duration_sum += event.duration_ms

        m = self._by_module.setdefault(
            event.module,
            {"total": 0, "success": 0, "error": 0, "skipped": 0, "duration_sum": 0.0},
        )
        m["total"] += 1
        m[event.status] = m.get(event.status, 0) + 1
        m["duration_sum"] += event.duration_ms

        route = event.data.get("route")
        if route:
            self._by_route[route] = self._by_route.get(route, 0) + 1

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

    @property
    def stats(self) -> dict:
        total = self._totals["total"]
        success = self._totals.get("success", 0)
        return {
            "total": total,
            "success": success,
            "error": self._totals.get("error", 0),
            "skipped": self._totals.get("skipped", 0),
            "success_rate": round(success / total * 100, 1) if total else 0.0,
            "avg_duration_ms": round(self._duration_sum / total, 2) if total else 0.0,
            "by_route": dict(sorted(self._by_route.items(), key=lambda x: -x[1])),
            "by_module": {
                name: {
                    "total": s["total"],
                    "success": s.get("success", 0),
                    "error": s.get("error", 0),
                    "avg_ms": round(s["duration_sum"] / s["total"], 2) if s["total"] else 0.0,
                }
                for name, s in self._by_module.items()
            },
        }


store = EventStore()
