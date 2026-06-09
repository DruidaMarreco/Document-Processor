"""Tests for POST /webhooks/{id}/deliveries/{delivery_id}/replay."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient

from document_processor import storage
from document_processor.api import app
from document_processor.models import PipelineResult, StageResult, WebhookConfig

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


async def _upload(client, content: bytes = _INVOICE) -> str:
    r = await client.post("/process", files={"file": ("doc.txt", content, "text/plain")})
    assert r.status_code == 200
    return r.json()["document_id"]


async def _log_delivery(webhook_id: str, doc_id: str, success: bool = True) -> dict:
    await storage.log_webhook_delivery(
        webhook_id=webhook_id,
        event="document.processed",
        document_id=doc_id,
        attempt=1,
        status_code=200 if success else 500,
        success=success,
        error=None if success else "Internal Server Error",
    )
    deliveries, _ = await storage.get_webhook_deliveries(webhook_id)
    return deliveries[0]


# ---------------------------------------------------------------------------
# Storage unit tests — get_webhook_delivery
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_delivery_by_id():
    async with _client() as c:
        wh_id = await _create_webhook(c)
        doc_id = await _upload(c)
    delivery = await _log_delivery(wh_id, doc_id)
    fetched = await storage.get_webhook_delivery(delivery["id"])
    assert fetched is not None
    assert fetched["id"] == delivery["id"]
    assert fetched["webhook_id"] == wh_id
    assert fetched["event"] == "document.processed"
    assert fetched["document_id"] == doc_id
    assert fetched["success"] is True


@pytest.mark.asyncio
async def test_get_delivery_not_found():
    result = await storage.get_webhook_delivery(99999)
    assert result is None


@pytest.mark.asyncio
async def test_get_delivery_has_all_fields():
    async with _client() as c:
        wh_id = await _create_webhook(c)
        doc_id = await _upload(c)
    delivery = await _log_delivery(wh_id, doc_id, success=False)
    fetched = await storage.get_webhook_delivery(delivery["id"])
    for field in ("id", "webhook_id", "event", "document_id", "attempt",
                  "status_code", "success", "error", "delivered_at"):
        assert field in fetched


# ---------------------------------------------------------------------------
# API endpoint tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_replay_404_webhook_missing():
    async with _client() as c:
        r = await c.post(f"/webhooks/{uuid4()}/deliveries/1/replay")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_replay_404_delivery_missing():
    async with _client() as c:
        wh_id = await _create_webhook(c)
        r = await c.post(f"/webhooks/{wh_id}/deliveries/99999/replay")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_replay_404_delivery_belongs_to_other_webhook():
    async with _client() as c:
        wh1_id = await _create_webhook(c, "https://a.example.com/hook")
        wh2_id = await _create_webhook(c, "https://b.example.com/hook")
        doc_id = await _upload(c)
    delivery = await _log_delivery(wh1_id, doc_id)
    async with _client() as c:
        # Try to replay a delivery from wh1 via wh2's endpoint
        r = await c.post(f"/webhooks/{wh2_id}/deliveries/{delivery['id']}/replay")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_replay_422_document_deleted():
    async with _client() as c:
        wh_id = await _create_webhook(c)
    # Log a delivery referencing a document that no longer exists
    await storage.log_webhook_delivery(
        webhook_id=wh_id,
        event="document.processed",
        document_id=str(uuid4()),
        attempt=1,
        status_code=200,
        success=True,
    )
    deliveries, _ = await storage.get_webhook_deliveries(wh_id)
    delivery_id = deliveries[0]["id"]
    async with _client() as c:
        r = await c.post(f"/webhooks/{wh_id}/deliveries/{delivery_id}/replay")
    assert r.status_code == 422


@pytest.mark.asyncio
async def test_replay_200_on_success():
    async with _client() as c:
        wh_id = await _create_webhook(c)
        doc_id = await _upload(c)
    delivery = await _log_delivery(wh_id, doc_id)

    async def fake_deliver(wh, event, payload, document_id=None):
        return True

    with patch("document_processor.webhook_delivery._deliver_one", new=fake_deliver):
        async with _client() as c:
            r = await c.post(f"/webhooks/{wh_id}/deliveries/{delivery['id']}/replay")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_replay_response_structure():
    async with _client() as c:
        wh_id = await _create_webhook(c)
        doc_id = await _upload(c)
    delivery = await _log_delivery(wh_id, doc_id)

    async def fake_deliver(wh, event, payload, document_id=None):
        return True

    with patch("document_processor.webhook_delivery._deliver_one", new=fake_deliver):
        async with _client() as c:
            r = await c.post(f"/webhooks/{wh_id}/deliveries/{delivery['id']}/replay")
    body = r.json()
    assert body["webhook_id"] == wh_id
    assert body["original_delivery_id"] == delivery["id"]
    assert body["event"] == "document.processed"
    assert body["document_id"] == doc_id
    assert "success" in body


@pytest.mark.asyncio
async def test_replay_success_reflects_deliver_result():
    async with _client() as c:
        wh_id = await _create_webhook(c)
        doc_id = await _upload(c)
    delivery = await _log_delivery(wh_id, doc_id)

    async def fake_deliver_fail(wh, event, payload, document_id=None):
        return False

    with patch("document_processor.webhook_delivery._deliver_one", new=fake_deliver_fail):
        async with _client() as c:
            r = await c.post(f"/webhooks/{wh_id}/deliveries/{delivery['id']}/replay")
    assert r.json()["success"] is False


@pytest.mark.asyncio
async def test_replay_calls_deliver_with_correct_event():
    async with _client() as c:
        wh_id = await _create_webhook(c)
        doc_id = await _upload(c)
    delivery = await _log_delivery(wh_id, doc_id)

    captured = {}

    async def fake_deliver(wh, event, payload, document_id=None):
        captured["event"] = event
        captured["document_id"] = document_id
        return True

    with patch("document_processor.webhook_delivery._deliver_one", new=fake_deliver):
        async with _client() as c:
            await c.post(f"/webhooks/{wh_id}/deliveries/{delivery['id']}/replay")

    assert captured["event"] == "document.processed"
    assert captured["document_id"] == doc_id


@pytest.mark.asyncio
async def test_replay_logs_new_delivery():
    """Replaying a delivery produces a new entry in the delivery log."""
    async with _client() as c:
        wh_id = await _create_webhook(c)
        doc_id = await _upload(c)
    delivery = await _log_delivery(wh_id, doc_id)

    async def fake_deliver(wh, event, payload, document_id=None):
        return True

    with patch("document_processor.webhook_delivery._deliver_one", new=fake_deliver):
        async with _client() as c:
            await c.post(f"/webhooks/{wh_id}/deliveries/{delivery['id']}/replay")

    deliveries, total = await storage.get_webhook_deliveries(wh_id)
    # original + replay = 2 deliveries
    assert total >= 2


@pytest.mark.asyncio
async def test_replay_works_on_failed_delivery():
    """Can replay a delivery that originally failed."""
    async with _client() as c:
        wh_id = await _create_webhook(c)
        doc_id = await _upload(c)
    delivery = await _log_delivery(wh_id, doc_id, success=False)

    async def fake_deliver(wh, event, payload, document_id=None):
        return True

    with patch("document_processor.webhook_delivery._deliver_one", new=fake_deliver):
        async with _client() as c:
            r = await c.post(f"/webhooks/{wh_id}/deliveries/{delivery['id']}/replay")
    assert r.status_code == 200
    assert r.json()["success"] is True


@pytest.mark.asyncio
async def test_replay_works_on_paused_webhook():
    """Replay is explicit — it works even if the webhook is paused."""
    async with _client() as c:
        wh_id = await _create_webhook(c)
        doc_id = await _upload(c)
        await c.post(f"/webhooks/{wh_id}/pause")
    delivery = await _log_delivery(wh_id, doc_id)

    async def fake_deliver(wh, event, payload, document_id=None):
        return True

    with patch("document_processor.webhook_delivery._deliver_one", new=fake_deliver):
        async with _client() as c:
            r = await c.post(f"/webhooks/{wh_id}/deliveries/{delivery['id']}/replay")
    assert r.status_code == 200
