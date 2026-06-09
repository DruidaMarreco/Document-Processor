"""Tests for result locking — POST /results/{id}/lock|unlock and lock enforcement."""
from __future__ import annotations

from pathlib import Path
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


async def _upload(client, content: bytes = _INVOICE) -> str:
    r = await client.post("/process", files={"file": ("doc.txt", content, "text/plain")})
    assert r.status_code == 200
    return r.json()["document_id"]


# ---------------------------------------------------------------------------
# Storage unit tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_result_unlocked_by_default():
    async with _client() as c:
        doc_id = await _upload(c)
    state = await storage.is_result_locked(__import__("uuid").UUID(doc_id))
    assert state is False


@pytest.mark.asyncio
async def test_set_locked_true():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    updated = await storage.set_result_locked(uid, True)
    assert updated is True
    assert await storage.is_result_locked(uid) is True


@pytest.mark.asyncio
async def test_set_locked_false():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    await storage.set_result_locked(uid, True)
    await storage.set_result_locked(uid, False)
    assert await storage.is_result_locked(uid) is False


@pytest.mark.asyncio
async def test_set_locked_returns_false_for_missing():
    updated = await storage.set_result_locked(uuid4(), True)
    assert updated is False


@pytest.mark.asyncio
async def test_is_locked_returns_none_for_missing():
    state = await storage.is_result_locked(uuid4())
    assert state is None


# ---------------------------------------------------------------------------
# API — lock / unlock endpoints
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_lock_endpoint_204():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.post(f"/results/{doc_id}/lock")
    assert r.status_code == 204


@pytest.mark.asyncio
async def test_unlock_endpoint_204():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.post(f"/results/{doc_id}/lock")
        r = await c.post(f"/results/{doc_id}/unlock")
    assert r.status_code == 204


@pytest.mark.asyncio
async def test_get_lock_state_initially_false():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.get(f"/results/{doc_id}/lock")
    assert r.status_code == 200
    assert r.json()["locked"] is False


@pytest.mark.asyncio
async def test_get_lock_state_after_lock():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.post(f"/results/{doc_id}/lock")
        r = await c.get(f"/results/{doc_id}/lock")
    assert r.json()["locked"] is True


@pytest.mark.asyncio
async def test_get_lock_state_after_unlock():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.post(f"/results/{doc_id}/lock")
        await c.post(f"/results/{doc_id}/unlock")
        r = await c.get(f"/results/{doc_id}/lock")
    assert r.json()["locked"] is False


@pytest.mark.asyncio
async def test_lock_404_missing():
    async with _client() as c:
        r = await c.post(f"/results/{uuid4()}/lock")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_unlock_404_missing():
    async with _client() as c:
        r = await c.post(f"/results/{uuid4()}/unlock")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_get_lock_state_404_missing():
    async with _client() as c:
        r = await c.get(f"/results/{uuid4()}/lock")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_lock_idempotent():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.post(f"/results/{doc_id}/lock")
        r = await c.post(f"/results/{doc_id}/lock")
    assert r.status_code == 204


@pytest.mark.asyncio
async def test_unlock_idempotent():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.post(f"/results/{doc_id}/unlock")
    assert r.status_code == 204


@pytest.mark.asyncio
async def test_lock_recorded_in_audit():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.post(f"/results/{doc_id}/lock")
        r = await c.get(f"/results/{doc_id}/audit")
    actions = [e["action"] for e in r.json()["entries"]]
    assert "locked" in actions


@pytest.mark.asyncio
async def test_unlock_recorded_in_audit():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.post(f"/results/{doc_id}/lock")
        await c.post(f"/results/{doc_id}/unlock")
        r = await c.get(f"/results/{doc_id}/audit")
    actions = [e["action"] for e in r.json()["entries"]]
    assert "unlocked" in actions


# ---------------------------------------------------------------------------
# API — lock enforcement
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_delete_locked_result_returns_423():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.post(f"/results/{doc_id}/lock")
        r = await c.delete(f"/results/{doc_id}")
    assert r.status_code == 423


@pytest.mark.asyncio
async def test_delete_unlocked_result_succeeds():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.post(f"/results/{doc_id}/lock")
        await c.post(f"/results/{doc_id}/unlock")
        r = await c.delete(f"/results/{doc_id}")
    assert r.status_code == 204


@pytest.mark.asyncio
async def test_reprocess_locked_result_returns_423():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.post(f"/results/{doc_id}/lock")
        r = await c.post(f"/results/{doc_id}/reprocess")
    assert r.status_code == 423
