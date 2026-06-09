"""Tests for webhook pause/resume — POST /webhooks/{id}/pause|resume."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient

from document_processor import storage
from document_processor.api import app
from document_processor.models import PipelineResult, StageResult, WebhookConfig
from document_processor.webhook_delivery import fire_webhooks

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


def _make_result() -> PipelineResult:
    return PipelineResult(
        document_id=uuid4(),
        status="success",
        total_duration_ms=10.0,
        stages=[
            StageResult(module="classifier", status="success",
                        data={"type": "invoice", "confidence": 0.9}, duration_ms=1.0),
        ],
    )


# ---------------------------------------------------------------------------
# Storage unit tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_webhook_active_by_default():
    async with _client() as c:
        wh_id = await _create_webhook(c)
    whs = await storage.list_webhooks()
    wh = next(w for w in whs if str(w.id) == wh_id)
    assert wh.active is True


@pytest.mark.asyncio
async def test_set_webhook_inactive():
    async with _client() as c:
        wh_id = await _create_webhook(c)
    updated = await storage.set_webhook_active(__import__("uuid").UUID(wh_id), False)
    assert updated is True
    whs = await storage.list_webhooks()
    wh = next(w for w in whs if str(w.id) == wh_id)
    assert wh.active is False


@pytest.mark.asyncio
async def test_set_webhook_active_back():
    async with _client() as c:
        wh_id = await _create_webhook(c)
    uid = __import__("uuid").UUID(wh_id)
    await storage.set_webhook_active(uid, False)
    await storage.set_webhook_active(uid, True)
    whs = await storage.list_webhooks()
    wh = next(w for w in whs if str(w.id) == wh_id)
    assert wh.active is True


@pytest.mark.asyncio
async def test_set_webhook_active_returns_false_for_missing():
    updated = await storage.set_webhook_active(uuid4(), False)
    assert updated is False


@pytest.mark.asyncio
async def test_paused_webhook_not_in_active_only_list():
    async with _client() as c:
        wh_id = await _create_webhook(c)
    uid = __import__("uuid").UUID(wh_id)
    await storage.set_webhook_active(uid, False)
    active = await storage.list_webhooks(active_only=True)
    assert all(str(w.id) != wh_id for w in active)


@pytest.mark.asyncio
async def test_paused_webhook_not_fired():
    """fire_webhooks skips inactive webhooks."""
    async with _client() as c:
        wh_id = await _create_webhook(c)
    uid = __import__("uuid").UUID(wh_id)
    await storage.set_webhook_active(uid, False)

    whs = await storage.list_webhooks()
    result = _make_result()
    delivered: list = []

    async def fake_deliver(w, event, payload, document_id=None):
        delivered.append(str(w.id))
        return True

    with patch("document_processor.webhook_delivery._deliver_one", new=fake_deliver):
        await fire_webhooks(whs, "document.processed", result)

    assert wh_id not in delivered


@pytest.mark.asyncio
async def test_resumed_webhook_is_fired():
    """After resume, fire_webhooks delivers to the webhook again."""
    async with _client() as c:
        wh_id = await _create_webhook(c)
    uid = __import__("uuid").UUID(wh_id)
    await storage.set_webhook_active(uid, False)
    await storage.set_webhook_active(uid, True)

    whs = await storage.list_webhooks()
    result = _make_result()
    delivered: list = []

    async def fake_deliver(w, event, payload, document_id=None):
        delivered.append(str(w.id))
        return True

    with patch("document_processor.webhook_delivery._deliver_one", new=fake_deliver):
        await fire_webhooks(whs, "document.processed", result)

    assert wh_id in delivered


# ---------------------------------------------------------------------------
# API endpoint tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_pause_endpoint_204():
    async with _client() as c:
        wh_id = await _create_webhook(c)
        r = await c.post(f"/webhooks/{wh_id}/pause")
    assert r.status_code == 204


@pytest.mark.asyncio
async def test_pause_sets_active_false():
    async with _client() as c:
        wh_id = await _create_webhook(c)
        await c.post(f"/webhooks/{wh_id}/pause")
        whs = await c.get("/webhooks")
    wh = next(w for w in whs.json() if w["id"] == wh_id)
    assert wh["active"] is False


@pytest.mark.asyncio
async def test_pause_404_for_missing():
    async with _client() as c:
        r = await c.post(f"/webhooks/{uuid4()}/pause")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_resume_endpoint_204():
    async with _client() as c:
        wh_id = await _create_webhook(c)
        await c.post(f"/webhooks/{wh_id}/pause")
        r = await c.post(f"/webhooks/{wh_id}/resume")
    assert r.status_code == 204


@pytest.mark.asyncio
async def test_resume_sets_active_true():
    async with _client() as c:
        wh_id = await _create_webhook(c)
        await c.post(f"/webhooks/{wh_id}/pause")
        await c.post(f"/webhooks/{wh_id}/resume")
        whs = await c.get("/webhooks")
    wh = next(w for w in whs.json() if w["id"] == wh_id)
    assert wh["active"] is True


@pytest.mark.asyncio
async def test_resume_404_for_missing():
    async with _client() as c:
        r = await c.post(f"/webhooks/{uuid4()}/resume")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_pause_idempotent():
    async with _client() as c:
        wh_id = await _create_webhook(c)
        await c.post(f"/webhooks/{wh_id}/pause")
        r = await c.post(f"/webhooks/{wh_id}/pause")
    assert r.status_code == 204


@pytest.mark.asyncio
async def test_resume_idempotent():
    async with _client() as c:
        wh_id = await _create_webhook(c)
        r = await c.post(f"/webhooks/{wh_id}/resume")
    assert r.status_code == 204
