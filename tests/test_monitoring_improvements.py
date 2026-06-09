"""Tests for improved monitoring: percentiles, throughput, quality scores, error buffer."""
from __future__ import annotations

import asyncio
import time

import pytest

from monitoring_module.store import EventStore, PipelineEvent, _percentile


def _event(
    module: str = "classifier",
    status: str = "success",
    duration_ms: float = 100.0,
    data: dict | None = None,
    document_id: str = "doc-1",
    errors: list[str] | None = None,
) -> PipelineEvent:
    return PipelineEvent(
        document_id=document_id,
        module=module,
        status=status,
        data=data or {},
        errors=errors or [],
        duration_ms=duration_ms,
    )


# ---------------------------------------------------------------------------
# _percentile helper
# ---------------------------------------------------------------------------

def test_percentile_p50():
    assert _percentile([1.0, 2.0, 3.0, 4.0, 5.0], 50) == 3.0


def test_percentile_p100():
    vals = [1.0, 2.0, 3.0]
    assert _percentile(vals, 100) == 3.0


def test_percentile_p0():
    vals = [1.0, 2.0, 3.0]
    assert _percentile(vals, 0) == 1.0


def test_percentile_empty():
    assert _percentile([], 95) == 0.0


def test_percentile_single():
    assert _percentile([42.0], 99) == 42.0


# ---------------------------------------------------------------------------
# Latency percentiles in stats
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_stats_latency_percentiles_present():
    s = EventStore()
    for ms in [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]:
        await s.record(_event(duration_ms=float(ms)))
    stats = s.stats
    lp = stats["latency_percentiles"]
    assert "p50" in lp and "p95" in lp and "p99" in lp


@pytest.mark.asyncio
async def test_stats_p50_reasonable():
    s = EventStore()
    for ms in range(1, 101):  # 1..100
        await s.record(_event(duration_ms=float(ms)))
    stats = s.stats
    p50 = stats["latency_percentiles"]["p50"]
    assert 45.0 <= p50 <= 55.0


@pytest.mark.asyncio
async def test_stats_p95_above_p50():
    s = EventStore()
    for ms in range(1, 101):
        await s.record(_event(duration_ms=float(ms)))
    stats = s.stats
    lp = stats["latency_percentiles"]
    assert lp["p95"] > lp["p50"]


@pytest.mark.asyncio
async def test_per_module_latency():
    s = EventStore()
    for ms in [10, 20, 30, 40, 50]:
        await s.record(_event(module="router", duration_ms=float(ms)))
    stats = s.stats
    router_stats = stats["by_module"].get("router", {})
    assert "latency" in router_stats
    assert "p50" in router_stats["latency"]
    assert router_stats["latency"]["p50"] == 30.0


# ---------------------------------------------------------------------------
# Throughput tracking
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_throughput_counts_recent():
    s = EventStore()
    for _ in range(5):
        await s.record(_event())
    assert s.throughput(window_seconds=60) == 5


@pytest.mark.asyncio
async def test_throughput_in_stats():
    s = EventStore()
    await s.record(_event())
    stats = s.stats
    assert "throughput" in stats
    assert "last_1m" in stats["throughput"]
    assert "last_5m" in stats["throughput"]
    assert "last_60m" in stats["throughput"]
    assert stats["throughput"]["last_1m"] >= 1


@pytest.mark.asyncio
async def test_throughput_zero_for_large_window_past():
    s = EventStore()
    # Manually inject an old timestamp
    s._event_times.append(time.time() - 7200)  # 2 hours ago
    assert s.throughput(window_seconds=3600) == 0


# ---------------------------------------------------------------------------
# Recent errors buffer
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_recent_errors_captures_errors():
    s = EventStore()
    await s.record(_event(status="success"))
    await s.record(_event(status="error", errors=["Something failed"]))
    await s.record(_event(status="error", errors=["Another failure"]))
    errors = s.recent_errors()
    assert len(errors) == 2
    assert all(e.status == "error" for e in errors)


@pytest.mark.asyncio
async def test_recent_errors_bounded():
    s = EventStore(maxlen=100)
    for i in range(60):
        await s.record(_event(status="error", document_id=f"doc-{i}"))
    # Buffer capped at 50
    errors = s.recent_errors()
    assert len(errors) <= 50


@pytest.mark.asyncio
async def test_recent_errors_empty_when_no_errors():
    s = EventStore()
    for _ in range(5):
        await s.record(_event(status="success"))
    assert s.recent_errors() == []


@pytest.mark.asyncio
async def test_recent_errors_respects_n():
    s = EventStore()
    for i in range(10):
        await s.record(_event(status="error", document_id=f"doc-{i}"))
    errors = s.recent_errors(n=3)
    assert len(errors) == 3


# ---------------------------------------------------------------------------
# Quality score tracking
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_quality_score_tracked():
    s = EventStore()
    await s.record(_event(
        module="generator",
        data={"output": {"quality_score": 80}},
    ))
    await s.record(_event(
        module="generator",
        data={"output": {"quality_score": 60}},
    ))
    stats = s.stats
    assert stats["avg_quality_score"] == 70.0


@pytest.mark.asyncio
async def test_quality_score_none_when_no_generator_events():
    s = EventStore()
    await s.record(_event(module="classifier"))
    assert s.stats["avg_quality_score"] is None


@pytest.mark.asyncio
async def test_quality_score_only_from_generator():
    s = EventStore()
    # Non-generator module with quality_score-like data should be ignored
    await s.record(_event(module="validator", data={"quality_score": 99}))
    assert s.stats["avg_quality_score"] is None
    # Real generator event
    await s.record(_event(module="generator", data={"output": {"quality_score": 50}}))
    assert s.stats["avg_quality_score"] == 50.0


# ---------------------------------------------------------------------------
# Prometheus metrics endpoint
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_prometheus_metrics_include_percentiles():
    from httpx import AsyncClient, ASGITransport
    from monitoring_module.api import app
    from monitoring_module import store as store_module

    s = EventStore()
    for ms in [10, 50, 90, 100]:
        await s.record(_event(duration_ms=float(ms)))

    original = store_module.store
    store_module.store = s
    # Patch api module's store reference
    import monitoring_module.api as api_mod
    api_mod.store = s
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.get("/metrics")
        assert resp.status_code == 200
        body = resp.text
        assert "doc_processor_duration_p50_ms" in body
        assert "doc_processor_duration_p95_ms" in body
        assert "doc_processor_throughput_1m" in body
    finally:
        store_module.store = original
        api_mod.store = original


@pytest.mark.asyncio
async def test_errors_endpoint():
    from httpx import AsyncClient, ASGITransport
    from monitoring_module.api import app
    from monitoring_module import store as store_module
    import monitoring_module.api as api_mod

    s = EventStore()
    await s.record(_event(status="error", errors=["test error"]))

    original = store_module.store
    store_module.store = s
    api_mod.store = s
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.get("/errors")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["status"] == "error"
    finally:
        store_module.store = original
        api_mod.store = original
