"""Tests for webhook delivery log — GET /webhooks/{id}/deliveries[/stats]."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient

from document_processor import storage
from document_processor.api import app
from document_processor.models import WebhookConfig

_INVOICE = b"Invoice No: 1234\nBill To: John\nAmount Due: $500\nDue Date: 2026-01-01"


@pytest.fixture(autouse=True)
def tmp_db(tmp_path: Path):
    storage.set_db_path(tmp_path / "test.db")
    yield
    storage.set_db_path(Path("data/results.db"))


def _client():
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


async def _create_webhook(client) -> str:
    r = await client.post("/webhooks", json={"url": "https://example.com/hook"})
    assert r.status_code == 201
    return r.json()["id"]


# ---------------------------------------------------------------------------
# Unit tests for storage helpers
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_log_and_retrieve_delivery():
    wh_id = str(uuid4())
    await storage.log_webhook_delivery(wh_id, "document.processed", "doc-1", 1, 200, True)
    deliveries, total = await storage.get_webhook_deliveries(wh_id)
    assert total == 1
    d = deliveries[0]
    assert d["webhook_id"] == wh_id
    assert d["event"] == "document.processed"
    assert d["document_id"] == "doc-1"
    assert d["attempt"] == 1
    assert d["status_code"] == 200
    assert d["success"] is True
    assert d["error"] is None


@pytest.mark.asyncio
async def test_log_failed_delivery_stores_error():
    wh_id = str(uuid4())
    await storage.log_webhook_delivery(wh_id, "document.processed", None, 2, 503, False, "server error")
    deliveries, _ = await storage.get_webhook_deliveries(wh_id)
    assert deliveries[0]["success"] is False
    assert deliveries[0]["error"] == "server error"
    assert deliveries[0]["status_code"] == 503


@pytest.mark.asyncio
async def test_delivery_stats_empty():
    wh_id = str(uuid4())
    stats = await storage.get_webhook_delivery_stats(wh_id)
    assert stats["total"] == 0
    assert stats["successes"] == 0
    assert stats["failures"] == 0
    assert stats["success_rate"] == 0.0


@pytest.mark.asyncio
async def test_delivery_stats_mixed():
    wh_id = str(uuid4())
    await storage.log_webhook_delivery(wh_id, "document.processed", None, 1, 200, True)
    await storage.log_webhook_delivery(wh_id, "document.processed", None, 2, 500, False)
    await storage.log_webhook_delivery(wh_id, "document.processed", None, 1, 200, True)
    stats = await storage.get_webhook_delivery_stats(wh_id)
    assert stats["total"] == 3
    assert stats["successes"] == 2
    assert stats["failures"] == 1
    assert abs(stats["success_rate"] - 2 / 3) < 1e-4


@pytest.mark.asyncio
async def test_deliveries_isolated_by_webhook_id():
    wh1 = str(uuid4())
    wh2 = str(uuid4())
    await storage.log_webhook_delivery(wh1, "document.processed", None, 1, 200, True)
    await storage.log_webhook_delivery(wh2, "document.processed", None, 1, 200, True)
    d1, t1 = await storage.get_webhook_deliveries(wh1)
    d2, t2 = await storage.get_webhook_deliveries(wh2)
    assert t1 == 1 and t2 == 1
    assert d1[0]["webhook_id"] == wh1
    assert d2[0]["webhook_id"] == wh2


# ---------------------------------------------------------------------------
# API endpoint tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_deliveries_endpoint_404_for_missing_webhook():
    async with _client() as c:
        r = await c.get(f"/webhooks/{uuid4()}/deliveries")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_deliveries_stats_endpoint_404_for_missing_webhook():
    async with _client() as c:
        r = await c.get(f"/webhooks/{uuid4()}/deliveries/stats")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_deliveries_endpoint_empty_for_new_webhook():
    async with _client() as c:
        wh_id = await _create_webhook(c)
        r = await c.get(f"/webhooks/{wh_id}/deliveries")
    assert r.status_code == 200
    body = r.json()
    assert body["total"] == 0
    assert body["deliveries"] == []
    assert body["webhook_id"] == wh_id


@pytest.mark.asyncio
async def test_stats_endpoint_empty_for_new_webhook():
    async with _client() as c:
        wh_id = await _create_webhook(c)
        r = await c.get(f"/webhooks/{wh_id}/deliveries/stats")
    assert r.status_code == 200
    body = r.json()
    assert body["total"] == 0
    assert body["success_rate"] == 0.0


@pytest.mark.asyncio
async def test_deliveries_logged_on_successful_fire():
    """fire_webhooks logs a delivery when the mock returns HTTP 200."""
    from document_processor.webhook_delivery import fire_webhooks
    from document_processor.models import PipelineResult, StageResult

    wh_id = str(uuid4())
    wh = WebhookConfig(id=__import__("uuid").UUID(wh_id), url="https://example.com/hook")

    result = PipelineResult(
        document_id=uuid4(),
        status="success",
        total_duration_ms=10.0,
        stages=[
            StageResult(module="classifier", status="success",
                        data={"type": "invoice", "confidence": 0.9}, duration_ms=1.0),
        ],
    )

    # Persist the webhook so the log function can be called
    await storage.save_webhook(wh)

    async def fake_deliver(w, event, payload, document_id=None):
        await storage.log_webhook_delivery(
            str(w.id), event, document_id, 1, 200, True
        )
        return True

    with patch("document_processor.webhook_delivery._deliver_one", new=fake_deliver):
        await fire_webhooks([wh], "document.processed", result)

    deliveries, total = await storage.get_webhook_deliveries(wh_id)
    assert total == 1
    assert deliveries[0]["success"] is True
    assert deliveries[0]["status_code"] == 200


@pytest.mark.asyncio
async def test_deliveries_response_structure():
    async with _client() as c:
        wh_id = await _create_webhook(c)

    await storage.log_webhook_delivery(wh_id, "document.processed", "abc", 1, 200, True)

    async with _client() as c:
        r = await c.get(f"/webhooks/{wh_id}/deliveries")
    assert r.status_code == 200
    body = r.json()
    assert "webhook_id" in body
    assert "total" in body
    assert "offset" in body
    assert "limit" in body
    assert "deliveries" in body
    d = body["deliveries"][0]
    assert "id" in d
    assert "event" in d
    assert "attempt" in d
    assert "success" in d
    assert "delivered_at" in d


@pytest.mark.asyncio
async def test_deliveries_pagination():
    wh_id = str(uuid4())
    for i in range(10):
        await storage.log_webhook_delivery(wh_id, "document.processed", None, 1, 200, True)

    deliveries, total = await storage.get_webhook_deliveries(wh_id, limit=3, offset=0)
    assert total == 10
    assert len(deliveries) == 3

    deliveries2, _ = await storage.get_webhook_deliveries(wh_id, limit=3, offset=3)
    ids1 = {d["id"] for d in deliveries}
    ids2 = {d["id"] for d in deliveries2}
    assert ids1.isdisjoint(ids2)


@pytest.mark.asyncio
async def test_stats_success_rate_all_success():
    wh_id = str(uuid4())
    for _ in range(5):
        await storage.log_webhook_delivery(wh_id, "document.processed", None, 1, 200, True)
    stats = await storage.get_webhook_delivery_stats(wh_id)
    assert stats["success_rate"] == 1.0
    assert stats["failures"] == 0
