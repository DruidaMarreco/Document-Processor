"""Tests for LLM processing cost tracking — GET /results/{id}/cost, GET /results/stats/cost."""
from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient

from document_processor import storage
from document_processor.api import app
from llm_client.client import get_llm_usage, start_usage_tracking

_INVOICE = b"Invoice No: 1234\nBill To: John\nAmount Due: $500\nDue Date: 2026-01-01"


@pytest.fixture(autouse=True)
def tmp_db(tmp_path: Path):
    storage.set_db_path(tmp_path / "test.db")
    yield
    storage.set_db_path(Path("data/results.db"))


def _client():
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


async def _upload(client) -> str:
    r = await client.post("/process", files={"file": ("doc.txt", _INVOICE, "text/plain")})
    assert r.status_code == 200
    return r.json()["document_id"]


# ---------------------------------------------------------------------------
# LLM usage tracking unit tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_start_usage_tracking_clears():
    start_usage_tracking()
    assert get_llm_usage() == []


@pytest.mark.asyncio
async def test_get_llm_usage_returns_list():
    start_usage_tracking()
    usage = get_llm_usage()
    assert isinstance(usage, list)


@pytest.mark.asyncio
async def test_llm_usage_default_empty():
    # Without start_usage_tracking(), get_llm_usage returns empty list
    usage = get_llm_usage()
    assert isinstance(usage, list)


# ---------------------------------------------------------------------------
# Storage unit tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_save_and_get_result_cost():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    cost = await storage.save_result_cost(uid, 100, 50, "claude-haiku-4-5")
    assert cost["input_tokens"] == 100
    assert cost["output_tokens"] == 50
    assert cost["estimated_cost_usd"] > 0.0
    fetched = await storage.get_result_cost(uid)
    assert fetched is not None
    assert fetched["input_tokens"] == 100


@pytest.mark.asyncio
async def test_get_result_cost_missing():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    cost = await storage.get_result_cost(uid)
    assert cost is None


@pytest.mark.asyncio
async def test_save_result_cost_upserts():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    await storage.save_result_cost(uid, 100, 50, "claude-haiku-4-5")
    await storage.save_result_cost(uid, 200, 100, "claude-haiku-4-5")
    cost = await storage.get_result_cost(uid)
    assert cost["input_tokens"] == 200


@pytest.mark.asyncio
async def test_estimated_cost_calculation():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    # 1M input + 1M output = $1.00 + $5.00 = $6.00
    cost = await storage.save_result_cost(uid, 1_000_000, 1_000_000, "claude-haiku-4-5")
    assert abs(cost["estimated_cost_usd"] - 6.0) < 0.001


@pytest.mark.asyncio
async def test_get_aggregate_cost_empty():
    agg = await storage.get_aggregate_cost()
    assert agg["tracked_results"] == 0
    assert agg["total_input_tokens"] == 0
    assert agg["total_estimated_cost_usd"] == 0.0


@pytest.mark.asyncio
async def test_get_aggregate_cost_with_data():
    async with _client() as c:
        doc_id1 = await _upload(c)
        doc_id2 = await _upload(c)
    uid1 = __import__("uuid").UUID(doc_id1)
    uid2 = __import__("uuid").UUID(doc_id2)
    await storage.save_result_cost(uid1, 100, 50, "claude-haiku-4-5")
    await storage.save_result_cost(uid2, 200, 100, "claude-haiku-4-5")
    agg = await storage.get_aggregate_cost()
    assert agg["tracked_results"] == 2
    assert agg["total_input_tokens"] == 300
    assert agg["total_output_tokens"] == 150


# ---------------------------------------------------------------------------
# API endpoint tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_cost_200():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.get(f"/results/{doc_id}/cost")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_get_cost_untracked_returns_zeros():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.get(f"/results/{doc_id}/cost")
    body = r.json()
    assert body["tracked"] is False
    assert body["input_tokens"] == 0
    assert body["estimated_cost_usd"] == 0.0


@pytest.mark.asyncio
async def test_get_cost_after_manual_set():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    await storage.save_result_cost(uid, 500, 200, "claude-haiku-4-5")
    async with _client() as c:
        r = await c.get(f"/results/{doc_id}/cost")
    body = r.json()
    assert body["tracked"] is True
    assert body["input_tokens"] == 500
    assert body["estimated_cost_usd"] > 0.0


@pytest.mark.asyncio
async def test_get_cost_404_missing():
    async with _client() as c:
        r = await c.get(f"/results/{uuid4()}/cost")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_get_cost_response_structure():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.get(f"/results/{doc_id}/cost")
    body = r.json()
    assert "result_id" in body
    assert "tracked" in body
    assert "input_tokens" in body
    assert "output_tokens" in body
    assert "estimated_cost_usd" in body


@pytest.mark.asyncio
async def test_aggregate_cost_200():
    async with _client() as c:
        r = await c.get("/results/stats/cost")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_aggregate_cost_structure():
    async with _client() as c:
        r = await c.get("/results/stats/cost")
    body = r.json()
    assert "tracked_results" in body
    assert "total_input_tokens" in body
    assert "total_output_tokens" in body
    assert "total_estimated_cost_usd" in body


@pytest.mark.asyncio
async def test_aggregate_cost_increases_with_data():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    await storage.save_result_cost(uid, 1000, 500, "claude-haiku-4-5")
    async with _client() as c:
        r = await c.get("/results/stats/cost")
    body = r.json()
    assert body["tracked_results"] >= 1
    assert body["total_input_tokens"] >= 1000
