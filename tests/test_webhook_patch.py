"""Tests for PATCH /webhooks/{id} and GET /webhooks/events."""
from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient

from document_processor import storage
from document_processor.api import app


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
# Storage unit tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_patch_webhook_url():
    async with _client() as c:
        wh_id = await _create_webhook(c)
    uid = __import__("uuid").UUID(wh_id)
    updated = await storage.patch_webhook(uid, url="https://new.example.com/hook")
    assert updated is not None
    assert updated.url == "https://new.example.com/hook"


@pytest.mark.asyncio
async def test_patch_webhook_events():
    async with _client() as c:
        wh_id = await _create_webhook(c)
    uid = __import__("uuid").UUID(wh_id)
    updated = await storage.patch_webhook(uid, events=["document.failed"])
    assert updated.events == ["document.failed"]


@pytest.mark.asyncio
async def test_patch_webhook_doc_types():
    async with _client() as c:
        wh_id = await _create_webhook(c)
    uid = __import__("uuid").UUID(wh_id)
    updated = await storage.patch_webhook(uid, doc_types=["invoice", "contract"])
    assert updated.doc_types == ["invoice", "contract"]


@pytest.mark.asyncio
async def test_patch_webhook_returns_none_for_missing():
    result = await storage.patch_webhook(uuid4(), url="https://x.com/hook")
    assert result is None


@pytest.mark.asyncio
async def test_patch_webhook_partial_updates_preserve_other_fields():
    async with _client() as c:
        wh_id = await _create_webhook(c, "https://original.com/hook")
    uid = __import__("uuid").UUID(wh_id)
    original = await storage.get_webhook(uid)
    # Patch only events
    updated = await storage.patch_webhook(uid, events=["ping"])
    assert updated.url == original.url  # URL unchanged
    assert updated.events == ["ping"]


@pytest.mark.asyncio
async def test_patch_webhook_persisted():
    async with _client() as c:
        wh_id = await _create_webhook(c)
    uid = __import__("uuid").UUID(wh_id)
    await storage.patch_webhook(uid, url="https://persisted.com/hook")
    fetched = await storage.get_webhook(uid)
    assert fetched.url == "https://persisted.com/hook"


# ---------------------------------------------------------------------------
# API endpoint tests — PATCH /webhooks/{id}
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_patch_webhook_200():
    async with _client() as c:
        wh_id = await _create_webhook(c)
        r = await c.patch(f"/webhooks/{wh_id}", json={"url": "https://new.com/hook"})
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_patch_webhook_updates_url():
    async with _client() as c:
        wh_id = await _create_webhook(c)
        r = await c.patch(f"/webhooks/{wh_id}", json={"url": "https://updated.com/hook"})
    assert r.json()["url"] == "https://updated.com/hook"


@pytest.mark.asyncio
async def test_patch_webhook_updates_events():
    async with _client() as c:
        wh_id = await _create_webhook(c)
        r = await c.patch(f"/webhooks/{wh_id}", json={"events": ["document.failed"]})
    assert r.json()["events"] == ["document.failed"]


@pytest.mark.asyncio
async def test_patch_webhook_updates_doc_types():
    async with _client() as c:
        wh_id = await _create_webhook(c)
        r = await c.patch(f"/webhooks/{wh_id}", json={"doc_types": ["invoice"]})
    assert r.json()["doc_types"] == ["invoice"]


@pytest.mark.asyncio
async def test_patch_webhook_404_missing():
    async with _client() as c:
        r = await c.patch(f"/webhooks/{uuid4()}", json={"url": "https://x.com/hook"})
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_patch_webhook_empty_body_no_change():
    async with _client() as c:
        wh_id = await _create_webhook(c, "https://original.com/hook")
        r = await c.patch(f"/webhooks/{wh_id}", json={})
    assert r.status_code == 200
    assert r.json()["url"] == "https://original.com/hook"


@pytest.mark.asyncio
async def test_patch_webhook_returns_full_config():
    async with _client() as c:
        wh_id = await _create_webhook(c)
        r = await c.patch(f"/webhooks/{wh_id}", json={"events": ["ping"]})
    body = r.json()
    assert "id" in body
    assert "url" in body
    assert "events" in body
    assert "active" in body


@pytest.mark.asyncio
async def test_patch_does_not_affect_active_state():
    async with _client() as c:
        wh_id = await _create_webhook(c)
        await c.post(f"/webhooks/{wh_id}/pause")
        r = await c.patch(f"/webhooks/{wh_id}", json={"url": "https://new.com/hook"})
    assert r.json()["active"] is False  # still paused


# ---------------------------------------------------------------------------
# API endpoint tests — GET /webhooks/events
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_webhook_events_200():
    async with _client() as c:
        r = await c.get("/webhooks/events")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_get_webhook_events_structure():
    async with _client() as c:
        r = await c.get("/webhooks/events")
    assert "events" in r.json()
    assert isinstance(r.json()["events"], list)


@pytest.mark.asyncio
async def test_get_webhook_events_includes_document_processed():
    async with _client() as c:
        r = await c.get("/webhooks/events")
    assert "document.processed" in r.json()["events"]


@pytest.mark.asyncio
async def test_get_webhook_events_includes_ping():
    async with _client() as c:
        r = await c.get("/webhooks/events")
    assert "ping" in r.json()["events"]
