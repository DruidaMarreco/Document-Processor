"""Tests for result expiry (TTL) — PUT/DELETE/GET /results/{id}/expiry, list filtering, purge."""
from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient

from document_processor import storage
from document_processor.api import app

_INVOICE = b"Invoice No: 1234\nBill To: John\nAmount Due: $500\nDue Date: 2026-01-01"
_EMAIL = b"From: alice@example.com\nTo: bob@example.com\nSubject: Meeting"

_FUTURE = "2099-01-01T00:00:00Z"
_PAST = "2000-01-01T00:00:00Z"


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
# Storage unit tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_expiry_none_by_default():
    async with _client() as c:
        doc_id = await _upload(c)
    exp = await storage.get_result_expiry(__import__("uuid").UUID(doc_id))
    assert exp is None


@pytest.mark.asyncio
async def test_set_result_expiry():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    updated = await storage.set_result_expiry(uid, _FUTURE)
    assert updated is True
    assert await storage.get_result_expiry(uid) == _FUTURE


@pytest.mark.asyncio
async def test_clear_result_expiry():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    await storage.set_result_expiry(uid, _FUTURE)
    await storage.set_result_expiry(uid, None)
    assert await storage.get_result_expiry(uid) is None


@pytest.mark.asyncio
async def test_set_expiry_returns_false_for_missing():
    updated = await storage.set_result_expiry(uuid4(), _FUTURE)
    assert updated is False


@pytest.mark.asyncio
async def test_expired_results_excluded_from_search_by_default():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL, "email.txt")
    await storage.set_result_expiry(__import__("uuid").UUID(id1), _PAST)
    results, total = await storage.search_results()
    assert total == 1
    assert str(results[0].document_id) == id2


@pytest.mark.asyncio
async def test_expired_results_included_with_flag():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL, "email.txt")
    await storage.set_result_expiry(__import__("uuid").UUID(id1), _PAST)
    results, total = await storage.search_results(include_expired=True)
    assert total == 2


@pytest.mark.asyncio
async def test_future_expiry_not_excluded():
    async with _client() as c:
        doc_id = await _upload(c)
    await storage.set_result_expiry(__import__("uuid").UUID(doc_id), _FUTURE)
    results, total = await storage.search_results()
    assert total == 1


@pytest.mark.asyncio
async def test_purge_expired_results():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL, "email.txt")
    await storage.set_result_expiry(__import__("uuid").UUID(id1), _PAST)
    deleted = await storage.purge_expired_results()
    assert deleted == 1
    results, total = await storage.search_results(include_expired=True)
    assert total == 1


# ---------------------------------------------------------------------------
# API endpoint tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_set_expiry_204():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.put(f"/results/{doc_id}/expiry", json={"expires_at": _FUTURE})
    assert r.status_code == 204


@pytest.mark.asyncio
async def test_clear_expiry_204():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.put(f"/results/{doc_id}/expiry", json={"expires_at": _FUTURE})
        r = await c.delete(f"/results/{doc_id}/expiry")
    assert r.status_code == 204


@pytest.mark.asyncio
async def test_get_expiry_initially_null():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.get(f"/results/{doc_id}/expiry")
    assert r.status_code == 200
    assert r.json()["expires_at"] is None


@pytest.mark.asyncio
async def test_get_expiry_after_set():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.put(f"/results/{doc_id}/expiry", json={"expires_at": _FUTURE})
        r = await c.get(f"/results/{doc_id}/expiry")
    assert r.json()["expires_at"] == _FUTURE


@pytest.mark.asyncio
async def test_get_expiry_after_clear():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.put(f"/results/{doc_id}/expiry", json={"expires_at": _FUTURE})
        await c.delete(f"/results/{doc_id}/expiry")
        r = await c.get(f"/results/{doc_id}/expiry")
    assert r.json()["expires_at"] is None


@pytest.mark.asyncio
async def test_set_expiry_404_missing():
    async with _client() as c:
        r = await c.put(f"/results/{uuid4()}/expiry", json={"expires_at": _FUTURE})
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_clear_expiry_404_missing():
    async with _client() as c:
        r = await c.delete(f"/results/{uuid4()}/expiry")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_get_expiry_404_missing():
    async with _client() as c:
        r = await c.get(f"/results/{uuid4()}/expiry")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_expired_hidden_from_list_by_default():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL, "email.txt")
        await c.put(f"/results/{id1}/expiry", json={"expires_at": _PAST})
        r = await c.get("/results")
    body = r.json()
    assert body["total"] == 1
    assert body["results"][0]["document_id"] == id2


@pytest.mark.asyncio
async def test_expired_visible_with_include_expired():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        await c.put(f"/results/{id1}/expiry", json={"expires_at": _PAST})
        r = await c.get("/results?include_expired=true")
    body = r.json()
    assert body["total"] == 1


@pytest.mark.asyncio
async def test_purge_endpoint():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL, "email.txt")
        await c.put(f"/results/{id1}/expiry", json={"expires_at": _PAST})
        r = await c.post("/admin/purge-expired")
    assert r.status_code == 200
    assert r.json()["deleted"] == 1


@pytest.mark.asyncio
async def test_purge_no_expired():
    async with _client() as c:
        await _upload(c)
        r = await c.post("/admin/purge-expired")
    assert r.json()["deleted"] == 0


@pytest.mark.asyncio
async def test_expiry_set_recorded_in_audit():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.put(f"/results/{doc_id}/expiry", json={"expires_at": _FUTURE})
        r = await c.get(f"/results/{doc_id}/audit")
    actions = [e["action"] for e in r.json()["entries"]]
    assert "expiry_set" in actions


@pytest.mark.asyncio
async def test_expiry_cleared_recorded_in_audit():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.put(f"/results/{doc_id}/expiry", json={"expires_at": _FUTURE})
        await c.delete(f"/results/{doc_id}/expiry")
        r = await c.get(f"/results/{doc_id}/audit")
    actions = [e["action"] for e in r.json()["entries"]]
    assert "expiry_cleared" in actions


@pytest.mark.asyncio
async def test_get_expiry_response_has_document_id():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.get(f"/results/{doc_id}/expiry")
    assert r.json()["document_id"] == doc_id
