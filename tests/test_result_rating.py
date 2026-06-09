"""Tests for result rating — PUT/GET/DELETE /results/{id}/rating."""
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


async def _upload(client) -> str:
    r = await client.post("/process", files={"file": ("doc.txt", _INVOICE, "text/plain")})
    assert r.status_code == 200
    return r.json()["document_id"]


# ---------------------------------------------------------------------------
# Storage unit tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_rating_initially_none():
    async with _client() as c:
        doc_id = await _upload(c)
    rating = await storage.get_result_rating(__import__("uuid").UUID(doc_id))
    assert rating is None


@pytest.mark.asyncio
async def test_set_rating_returns_true():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    result = await storage.set_result_rating(uid, 4)
    assert result is True


@pytest.mark.asyncio
async def test_get_rating_after_set():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    await storage.set_result_rating(uid, 3)
    rating = await storage.get_result_rating(uid)
    assert rating == 3


@pytest.mark.asyncio
async def test_set_rating_overwrites():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    await storage.set_result_rating(uid, 2)
    await storage.set_result_rating(uid, 5)
    assert await storage.get_result_rating(uid) == 5


@pytest.mark.asyncio
async def test_delete_rating_clears():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    await storage.set_result_rating(uid, 4)
    await storage.delete_result_rating(uid)
    assert await storage.get_result_rating(uid) is None


@pytest.mark.asyncio
async def test_set_rating_missing_returns_false():
    result = await storage.set_result_rating(uuid4(), 3)
    assert result is False


@pytest.mark.asyncio
async def test_all_valid_ratings():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    for r in (1, 2, 3, 4, 5):
        await storage.set_result_rating(uid, r)
        assert await storage.get_result_rating(uid) == r


# ---------------------------------------------------------------------------
# API endpoint tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_set_rating_204():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.put(f"/results/{doc_id}/rating", json={"rating": 5})
    assert r.status_code == 204


@pytest.mark.asyncio
async def test_set_rating_404_missing():
    async with _client() as c:
        r = await c.put(f"/results/{uuid4()}/rating", json={"rating": 3})
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_set_rating_out_of_range_422():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.put(f"/results/{doc_id}/rating", json={"rating": 6})
    assert r.status_code == 422


@pytest.mark.asyncio
async def test_set_rating_zero_422():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.put(f"/results/{doc_id}/rating", json={"rating": 0})
    assert r.status_code == 422


@pytest.mark.asyncio
async def test_get_rating_200():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.put(f"/results/{doc_id}/rating", json={"rating": 4})
        r = await c.get(f"/results/{doc_id}/rating")
    assert r.status_code == 200
    assert r.json()["rating"] == 4


@pytest.mark.asyncio
async def test_get_rating_null_when_unset():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.get(f"/results/{doc_id}/rating")
    assert r.json()["rating"] is None


@pytest.mark.asyncio
async def test_get_rating_404_missing():
    async with _client() as c:
        r = await c.get(f"/results/{uuid4()}/rating")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_delete_rating_204():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.put(f"/results/{doc_id}/rating", json={"rating": 3})
        r = await c.delete(f"/results/{doc_id}/rating")
    assert r.status_code == 204


@pytest.mark.asyncio
async def test_delete_rating_clears_value():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.put(f"/results/{doc_id}/rating", json={"rating": 3})
        await c.delete(f"/results/{doc_id}/rating")
        r = await c.get(f"/results/{doc_id}/rating")
    assert r.json()["rating"] is None


@pytest.mark.asyncio
async def test_delete_rating_404_missing():
    async with _client() as c:
        r = await c.delete(f"/results/{uuid4()}/rating")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_rating_set_recorded_in_audit():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.put(f"/results/{doc_id}/rating", json={"rating": 5})
        r = await c.get(f"/results/{doc_id}/audit")
    actions = [e["action"] for e in r.json()["entries"]]
    assert "rating_set" in actions


@pytest.mark.asyncio
async def test_rating_cleared_recorded_in_audit():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.put(f"/results/{doc_id}/rating", json={"rating": 2})
        await c.delete(f"/results/{doc_id}/rating")
        r = await c.get(f"/results/{doc_id}/audit")
    actions = [e["action"] for e in r.json()["entries"]]
    assert "rating_cleared" in actions
