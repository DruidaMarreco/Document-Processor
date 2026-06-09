"""Tests for result priority — PUT/DELETE/GET /results/{id}/priority and ?priority= filter."""
from __future__ import annotations

from pathlib import Path
from uuid import uuid4

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
# Storage unit tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_priority_none_by_default():
    async with _client() as c:
        doc_id = await _upload(c)
    p = await storage.get_result_priority(__import__("uuid").UUID(doc_id))
    assert p is None


@pytest.mark.asyncio
async def test_set_priority_high():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    updated = await storage.set_result_priority(uid, "high")
    assert updated is True
    assert await storage.get_result_priority(uid) == "high"


@pytest.mark.asyncio
async def test_set_priority_all_levels():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    for level in ("low", "medium", "high", "critical"):
        await storage.set_result_priority(uid, level)
        assert await storage.get_result_priority(uid) == level


@pytest.mark.asyncio
async def test_clear_priority():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    await storage.set_result_priority(uid, "high")
    await storage.set_result_priority(uid, None)
    assert await storage.get_result_priority(uid) is None


@pytest.mark.asyncio
async def test_set_priority_returns_false_for_missing():
    updated = await storage.set_result_priority(uuid4(), "high")
    assert updated is False


@pytest.mark.asyncio
async def test_search_results_priority_filter():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL, "email.txt")
    await storage.set_result_priority(__import__("uuid").UUID(id1), "critical")
    results, total = await storage.search_results(priority="critical")
    assert total == 1
    ids = [str(r.document_id) for r in results]
    assert id1 in ids
    assert id2 not in ids


# ---------------------------------------------------------------------------
# API endpoint tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_set_priority_204():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.put(f"/results/{doc_id}/priority", json={"priority": "high"})
    assert r.status_code == 204


@pytest.mark.asyncio
async def test_clear_priority_204():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.put(f"/results/{doc_id}/priority", json={"priority": "high"})
        r = await c.delete(f"/results/{doc_id}/priority")
    assert r.status_code == 204


@pytest.mark.asyncio
async def test_get_priority_initially_null():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.get(f"/results/{doc_id}/priority")
    assert r.status_code == 200
    assert r.json()["priority"] is None


@pytest.mark.asyncio
async def test_get_priority_after_set():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.put(f"/results/{doc_id}/priority", json={"priority": "critical"})
        r = await c.get(f"/results/{doc_id}/priority")
    assert r.json()["priority"] == "critical"


@pytest.mark.asyncio
async def test_get_priority_after_clear():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.put(f"/results/{doc_id}/priority", json={"priority": "high"})
        await c.delete(f"/results/{doc_id}/priority")
        r = await c.get(f"/results/{doc_id}/priority")
    assert r.json()["priority"] is None


@pytest.mark.asyncio
async def test_set_priority_invalid_value():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.put(f"/results/{doc_id}/priority", json={"priority": "urgent"})
    assert r.status_code == 400


@pytest.mark.asyncio
async def test_set_priority_404_missing():
    async with _client() as c:
        r = await c.put(f"/results/{uuid4()}/priority", json={"priority": "high"})
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_clear_priority_404_missing():
    async with _client() as c:
        r = await c.delete(f"/results/{uuid4()}/priority")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_get_priority_404_missing():
    async with _client() as c:
        r = await c.get(f"/results/{uuid4()}/priority")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_set_priority_idempotent():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.put(f"/results/{doc_id}/priority", json={"priority": "medium"})
        r = await c.put(f"/results/{doc_id}/priority", json={"priority": "medium"})
    assert r.status_code == 204


@pytest.mark.asyncio
async def test_clear_priority_idempotent():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.delete(f"/results/{doc_id}/priority")
    assert r.status_code == 204


@pytest.mark.asyncio
async def test_priority_recorded_in_audit():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.put(f"/results/{doc_id}/priority", json={"priority": "high"})
        r = await c.get(f"/results/{doc_id}/audit")
    actions = [e["action"] for e in r.json()["entries"]]
    assert "priority_set" in actions


@pytest.mark.asyncio
async def test_clear_priority_recorded_in_audit():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.put(f"/results/{doc_id}/priority", json={"priority": "high"})
        await c.delete(f"/results/{doc_id}/priority")
        r = await c.get(f"/results/{doc_id}/audit")
    actions = [e["action"] for e in r.json()["entries"]]
    assert "priority_cleared" in actions


@pytest.mark.asyncio
async def test_list_results_priority_filter():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL, "email.txt")
        await c.put(f"/results/{id1}/priority", json={"priority": "critical"})
        r = await c.get("/results?priority=critical")
    body = r.json()
    assert body["total"] == 1
    assert body["results"][0]["document_id"] == id1


@pytest.mark.asyncio
async def test_list_results_priority_filter_excludes_others():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL, "email.txt")
        await c.put(f"/results/{id1}/priority", json={"priority": "low"})
        await c.put(f"/results/{id2}/priority", json={"priority": "high"})
        r = await c.get("/results?priority=high")
    body = r.json()
    assert body["total"] == 1
    assert body["results"][0]["document_id"] == id2


@pytest.mark.asyncio
async def test_get_priority_response_has_document_id():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.get(f"/results/{doc_id}/priority")
    assert r.json()["document_id"] == doc_id
