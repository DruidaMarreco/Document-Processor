"""Tests for POST /webhooks/{id}/ping — test ping to verify webhook URL reachability."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient

from document_processor import storage
from document_processor.api import app

_INVOICE = b"Invoice No: 1234\nBill To: John\nAmount Due: $500\nDue Date: 2026-01-01"


@pytest.fixture(autouse=True)
def tmp_db(tmp_path: Path):
    storage.set_db_path(tmp_path / "test.db")
    yield
    storage.set_db_path(Path("data/results.db"))


def _client():
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


async def _create_webhook(client, url: str = "https://example.com/hook") -> str:
    r = await client.post("/webhooks", json={"url": url})
    assert r.status_code == 201
    return r.json()["id"]


# ---------------------------------------------------------------------------
# API endpoint tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_ping_endpoint_200():
    async with _client() as c:
        wh_id = await _create_webhook(c)
    async def fake_deliver(wh, event, payload, document_id=None):
        return True
    with patch("document_processor.webhook_delivery._deliver_one", new=fake_deliver):
        async with _client() as c:
            r = await c.post(f"/webhooks/{wh_id}/ping")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_ping_response_structure():
    async with _client() as c:
        wh_id = await _create_webhook(c)
    async def fake_deliver(wh, event, payload, document_id=None):
        return True
    with patch("document_processor.webhook_delivery._deliver_one", new=fake_deliver):
        async with _client() as c:
            r = await c.post(f"/webhooks/{wh_id}/ping")
    body = r.json()
    assert "webhook_id" in body
    assert "success" in body


@pytest.mark.asyncio
async def test_ping_webhook_id_matches():
    async with _client() as c:
        wh_id = await _create_webhook(c)
    async def fake_deliver(wh, event, payload, document_id=None):
        return True
    with patch("document_processor.webhook_delivery._deliver_one", new=fake_deliver):
        async with _client() as c:
            r = await c.post(f"/webhooks/{wh_id}/ping")
    assert r.json()["webhook_id"] == wh_id


@pytest.mark.asyncio
async def test_ping_success_true_when_deliver_succeeds():
    async with _client() as c:
        wh_id = await _create_webhook(c)
    async def fake_deliver(wh, event, payload, document_id=None):
        return True
    with patch("document_processor.webhook_delivery._deliver_one", new=fake_deliver):
        async with _client() as c:
            r = await c.post(f"/webhooks/{wh_id}/ping")
    assert r.json()["success"] is True


@pytest.mark.asyncio
async def test_ping_success_false_when_deliver_fails():
    async with _client() as c:
        wh_id = await _create_webhook(c)
    async def fake_deliver(wh, event, payload, document_id=None):
        return False
    with patch("document_processor.webhook_delivery._deliver_one", new=fake_deliver):
        async with _client() as c:
            r = await c.post(f"/webhooks/{wh_id}/ping")
    assert r.json()["success"] is False


@pytest.mark.asyncio
async def test_ping_404_for_missing_webhook():
    async with _client() as c:
        r = await c.post(f"/webhooks/{uuid4()}/ping")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_ping_calls_deliver_with_ping_event():
    async with _client() as c:
        wh_id = await _create_webhook(c)
    captured = {}
    async def fake_deliver(wh, event, payload, document_id=None):
        captured["event"] = event
        captured["document_id"] = document_id
        return True
    with patch("document_processor.webhook_delivery._deliver_one", new=fake_deliver):
        async with _client() as c:
            await c.post(f"/webhooks/{wh_id}/ping")
    assert captured["event"] == "ping"
    assert captured["document_id"] is None


@pytest.mark.asyncio
async def test_ping_payload_contains_required_fields():
    async with _client() as c:
        wh_id = await _create_webhook(c)
    import json
    captured_payload = {}
    async def fake_deliver(wh, event, payload, document_id=None):
        captured_payload.update(json.loads(payload))
        return True
    with patch("document_processor.webhook_delivery._deliver_one", new=fake_deliver):
        async with _client() as c:
            await c.post(f"/webhooks/{wh_id}/ping")
    assert captured_payload["event"] == "ping"
    assert captured_payload["webhook_id"] == wh_id
    assert "timestamp" in captured_payload
    assert "message" in captured_payload


@pytest.mark.asyncio
async def test_ping_works_on_paused_webhook():
    """Ping is a diagnostic action — it works even when the webhook is paused."""
    async with _client() as c:
        wh_id = await _create_webhook(c)
        await c.post(f"/webhooks/{wh_id}/pause")
    async def fake_deliver(wh, event, payload, document_id=None):
        return True
    with patch("document_processor.webhook_delivery._deliver_one", new=fake_deliver):
        async with _client() as c:
            r = await c.post(f"/webhooks/{wh_id}/ping")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_ping_does_not_require_active_webhook():
    """Ping works regardless of whether the webhook is active or paused."""
    async with _client() as c:
        wh_id = await _create_webhook(c)
        # Check both active and inactive
        for _ in range(2):
            async def fake_deliver(wh, event, payload, document_id=None):
                return True
            with patch("document_processor.webhook_delivery._deliver_one", new=fake_deliver):
                r = await c.post(f"/webhooks/{wh_id}/ping")
            assert r.status_code == 200
            await c.post(f"/webhooks/{wh_id}/pause")


@pytest.mark.asyncio
async def test_ping_multiple_webhooks_independent():
    async with _client() as c:
        wh1 = await _create_webhook(c, "https://a.example.com/hook")
        wh2 = await _create_webhook(c, "https://b.example.com/hook")
    async def fake_deliver(wh, event, payload, document_id=None):
        return True
    with patch("document_processor.webhook_delivery._deliver_one", new=fake_deliver):
        async with _client() as c:
            r1 = await c.post(f"/webhooks/{wh1}/ping")
            r2 = await c.post(f"/webhooks/{wh2}/ping")
    assert r1.json()["webhook_id"] == wh1
    assert r2.json()["webhook_id"] == wh2
