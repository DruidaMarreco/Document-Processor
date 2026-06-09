"""Tests for enhanced processing statistics endpoints."""
from __future__ import annotations

from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

from document_processor import storage
from document_processor.api import app

_INVOICE = b"Invoice No: 1234\nBill To: John\nAmount Due: $500\nDue Date: 2026-01-01"
_EMAIL = b"From: alice@example.com\nTo: bob@example.com\nSubject: Meeting"


@pytest.fixture(autouse=True)
def tmp_db(tmp_path: Path):
    storage.set_db_path(tmp_path / "test.db")
    yield
    storage.set_db_path(Path("data/results.db"))


def _client():
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


async def _upload(client, content: bytes = _INVOICE, filename: str = "doc.txt") -> str:
    r = await client.post("/process", files={"file": (filename, content, "text/plain")})
    assert r.status_code == 200
    return r.json()["document_id"]


# ---------------------------------------------------------------------------
# Storage unit tests — timeline
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_timeline_empty_db():
    result = await storage.get_stats_timeline(days=30)
    assert result == []


@pytest.mark.asyncio
async def test_timeline_returns_today_after_upload():
    async with _client() as c:
        await _upload(c)
    timeline = await storage.get_stats_timeline(days=1)
    assert len(timeline) >= 1
    assert timeline[0]["count"] >= 1
    assert "date" in timeline[0]


@pytest.mark.asyncio
async def test_timeline_count_reflects_uploads():
    async with _client() as c:
        await _upload(c, _INVOICE)
        await _upload(c, _EMAIL, "email.txt")
    timeline = await storage.get_stats_timeline(days=1)
    total = sum(d["count"] for d in timeline)
    assert total == 2


@pytest.mark.asyncio
async def test_timeline_date_format():
    async with _client() as c:
        await _upload(c)
    timeline = await storage.get_stats_timeline(days=7)
    for entry in timeline:
        # Date should be YYYY-MM-DD
        parts = entry["date"].split("-")
        assert len(parts) == 3
        assert len(parts[0]) == 4


@pytest.mark.asyncio
async def test_timeline_newest_first():
    async with _client() as c:
        await _upload(c)
    timeline = await storage.get_stats_timeline(days=30)
    dates = [e["date"] for e in timeline]
    assert dates == sorted(dates, reverse=True)


# ---------------------------------------------------------------------------
# Storage unit tests — stage timing
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_stage_timing_empty_db():
    result = await storage.get_stats_stage_timing()
    assert result == []


@pytest.mark.asyncio
async def test_stage_timing_has_expected_modules():
    async with _client() as c:
        await _upload(c)
    timing = await storage.get_stats_stage_timing()
    modules = {t["module"] for t in timing}
    assert "classifier" in modules
    assert "extractor" in modules
    assert "validator" in modules
    assert "generator" in modules


@pytest.mark.asyncio
async def test_stage_timing_has_required_fields():
    async with _client() as c:
        await _upload(c)
    timing = await storage.get_stats_stage_timing()
    for entry in timing:
        assert "module" in entry
        assert "count" in entry
        assert "avg_ms" in entry
        assert "min_ms" in entry
        assert "max_ms" in entry
        assert "p50_ms" in entry
        assert "p95_ms" in entry


@pytest.mark.asyncio
async def test_stage_timing_values_are_non_negative():
    async with _client() as c:
        await _upload(c)
    timing = await storage.get_stats_stage_timing()
    for entry in timing:
        assert entry["avg_ms"] >= 0
        assert entry["min_ms"] >= 0
        assert entry["max_ms"] >= entry["min_ms"]
        assert entry["p50_ms"] >= 0
        assert entry["p95_ms"] >= entry["p50_ms"]


@pytest.mark.asyncio
async def test_stage_timing_count_increases_with_more_results():
    async with _client() as c:
        await _upload(c, _INVOICE)
        await _upload(c, _EMAIL, "email.txt")
    timing = await storage.get_stats_stage_timing()
    classifier = next(t for t in timing if t["module"] == "classifier")
    assert classifier["count"] == 2


# ---------------------------------------------------------------------------
# Storage unit tests — error rates
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_error_rates_empty_db():
    result = await storage.get_stats_error_rates()
    assert result == []


@pytest.mark.asyncio
async def test_error_rates_has_required_fields():
    async with _client() as c:
        await _upload(c)
    rates = await storage.get_stats_error_rates()
    for entry in rates:
        assert "doc_type" in entry
        assert "total" in entry
        assert "successes" in entry
        assert "failures" in entry
        assert "error_rate" in entry


@pytest.mark.asyncio
async def test_error_rates_success_rate_between_0_and_1():
    async with _client() as c:
        await _upload(c)
    rates = await storage.get_stats_error_rates()
    for entry in rates:
        assert 0.0 <= entry["error_rate"] <= 1.0


@pytest.mark.asyncio
async def test_error_rates_counts_add_up():
    async with _client() as c:
        await _upload(c)
    rates = await storage.get_stats_error_rates()
    for entry in rates:
        assert entry["successes"] + entry["failures"] == entry["total"]


# ---------------------------------------------------------------------------
# API endpoint tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_timeline_endpoint_200():
    async with _client() as c:
        r = await c.get("/results/stats/timeline")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_timeline_endpoint_response_structure():
    async with _client() as c:
        r = await c.get("/results/stats/timeline")
    body = r.json()
    assert "days" in body
    assert "timeline" in body
    assert isinstance(body["timeline"], list)
    assert body["days"] == 30  # default


@pytest.mark.asyncio
async def test_timeline_endpoint_custom_days():
    async with _client() as c:
        r = await c.get("/results/stats/timeline?days=7")
    assert r.json()["days"] == 7


@pytest.mark.asyncio
async def test_timeline_endpoint_days_out_of_range():
    async with _client() as c:
        r = await c.get("/results/stats/timeline?days=0")
    assert r.status_code == 422


@pytest.mark.asyncio
async def test_stage_timing_endpoint_200():
    async with _client() as c:
        r = await c.get("/results/stats/stage-timing")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_stage_timing_endpoint_response_structure():
    async with _client() as c:
        r = await c.get("/results/stats/stage-timing")
    body = r.json()
    assert "stages" in body
    assert isinstance(body["stages"], list)


@pytest.mark.asyncio
async def test_stage_timing_endpoint_populated_after_upload():
    async with _client() as c:
        await _upload(c)
        r = await c.get("/results/stats/stage-timing")
    assert len(r.json()["stages"]) > 0


@pytest.mark.asyncio
async def test_error_rates_endpoint_200():
    async with _client() as c:
        r = await c.get("/results/stats/error-rates")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_error_rates_endpoint_response_structure():
    async with _client() as c:
        r = await c.get("/results/stats/error-rates")
    body = r.json()
    assert "error_rates" in body
    assert isinstance(body["error_rates"], list)


@pytest.mark.asyncio
async def test_error_rates_endpoint_populated_after_upload():
    async with _client() as c:
        await _upload(c)
        r = await c.get("/results/stats/error-rates")
    assert len(r.json()["error_rates"]) > 0
