"""Tests for result starring — PUT/DELETE/GET /results/{id}/star and ?starred= filter."""
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
async def test_result_not_starred_by_default():
    async with _client() as c:
        doc_id = await _upload(c)
    state = await storage.is_starred(__import__("uuid").UUID(doc_id))
    assert state is False


@pytest.mark.asyncio
async def test_set_starred_true():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    updated = await storage.set_starred(uid, True)
    assert updated is True
    assert await storage.is_starred(uid) is True


@pytest.mark.asyncio
async def test_set_starred_false():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    await storage.set_starred(uid, True)
    await storage.set_starred(uid, False)
    assert await storage.is_starred(uid) is False


@pytest.mark.asyncio
async def test_set_starred_returns_false_for_missing():
    updated = await storage.set_starred(uuid4(), True)
    assert updated is False


@pytest.mark.asyncio
async def test_is_starred_returns_none_for_missing():
    state = await storage.is_starred(uuid4())
    assert state is None


@pytest.mark.asyncio
async def test_search_results_starred_filter():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL, "email.txt")
    await storage.set_starred(__import__("uuid").UUID(id1), True)
    results, total = await storage.search_results(starred=True)
    assert total == 1
    ids = [str(r.document_id) for r in results]
    assert id1 in ids
    assert id2 not in ids


@pytest.mark.asyncio
async def test_search_results_unstarred_filter():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL, "email.txt")
    await storage.set_starred(__import__("uuid").UUID(id1), True)
    results, total = await storage.search_results(starred=False)
    assert total == 1
    ids = [str(r.document_id) for r in results]
    assert id2 in ids
    assert id1 not in ids


# ---------------------------------------------------------------------------
# API endpoint tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_star_endpoint_204():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.put(f"/results/{doc_id}/star")
    assert r.status_code == 204


@pytest.mark.asyncio
async def test_unstar_endpoint_204():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.put(f"/results/{doc_id}/star")
        r = await c.delete(f"/results/{doc_id}/star")
    assert r.status_code == 204


@pytest.mark.asyncio
async def test_get_star_state_initially_false():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.get(f"/results/{doc_id}/star")
    assert r.status_code == 200
    assert r.json()["starred"] is False


@pytest.mark.asyncio
async def test_get_star_state_after_star():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.put(f"/results/{doc_id}/star")
        r = await c.get(f"/results/{doc_id}/star")
    assert r.json()["starred"] is True


@pytest.mark.asyncio
async def test_get_star_state_after_unstar():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.put(f"/results/{doc_id}/star")
        await c.delete(f"/results/{doc_id}/star")
        r = await c.get(f"/results/{doc_id}/star")
    assert r.json()["starred"] is False


@pytest.mark.asyncio
async def test_star_404_missing():
    async with _client() as c:
        r = await c.put(f"/results/{uuid4()}/star")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_unstar_404_missing():
    async with _client() as c:
        r = await c.delete(f"/results/{uuid4()}/star")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_get_star_state_404_missing():
    async with _client() as c:
        r = await c.get(f"/results/{uuid4()}/star")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_star_idempotent():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.put(f"/results/{doc_id}/star")
        r = await c.put(f"/results/{doc_id}/star")
    assert r.status_code == 204


@pytest.mark.asyncio
async def test_unstar_idempotent():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.delete(f"/results/{doc_id}/star")
    assert r.status_code == 204


@pytest.mark.asyncio
async def test_star_recorded_in_audit():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.put(f"/results/{doc_id}/star")
        r = await c.get(f"/results/{doc_id}/audit")
    actions = [e["action"] for e in r.json()["entries"]]
    assert "starred" in actions


@pytest.mark.asyncio
async def test_unstar_recorded_in_audit():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.put(f"/results/{doc_id}/star")
        await c.delete(f"/results/{doc_id}/star")
        r = await c.get(f"/results/{doc_id}/audit")
    actions = [e["action"] for e in r.json()["entries"]]
    assert "unstarred" in actions


@pytest.mark.asyncio
async def test_list_results_starred_filter():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL, "email.txt")
        await c.put(f"/results/{id1}/star")
        r = await c.get("/results?starred=true")
    body = r.json()
    assert body["total"] == 1
    assert body["results"][0]["document_id"] == id1


@pytest.mark.asyncio
async def test_list_results_unstarred_filter():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL, "email.txt")
        await c.put(f"/results/{id1}/star")
        r = await c.get("/results?starred=false")
    body = r.json()
    assert body["total"] == 1
    assert body["results"][0]["document_id"] == id2
