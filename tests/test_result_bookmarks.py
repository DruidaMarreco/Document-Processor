"""Tests for named result bookmarks — PUT/GET/DELETE /results/{id}/bookmarks/{name}."""
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
async def test_bookmarks_empty_initially():
    async with _client() as c:
        doc_id = await _upload(c)
    bms = await storage.get_bookmarks(__import__("uuid").UUID(doc_id))
    assert bms == []


@pytest.mark.asyncio
async def test_upsert_bookmark_creates():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    bm = await storage.upsert_bookmark(uid, "intro", "page:1", "Opening paragraph")
    assert bm["name"] == "intro"
    assert bm["reference"] == "page:1"
    assert bm["note"] == "Opening paragraph"


@pytest.mark.asyncio
async def test_upsert_bookmark_updates():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    await storage.upsert_bookmark(uid, "intro", "page:1")
    bm = await storage.upsert_bookmark(uid, "intro", "page:2", "Updated")
    assert bm["reference"] == "page:2"
    bms = await storage.get_bookmarks(uid)
    assert len(bms) == 1


@pytest.mark.asyncio
async def test_get_bookmark():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    await storage.upsert_bookmark(uid, "sec1", "section:1")
    bm = await storage.get_bookmark(uid, "sec1")
    assert bm is not None
    assert bm["name"] == "sec1"


@pytest.mark.asyncio
async def test_get_bookmark_missing():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    bm = await storage.get_bookmark(uid, "ghost")
    assert bm is None


@pytest.mark.asyncio
async def test_delete_bookmark():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    await storage.upsert_bookmark(uid, "tmp", "line:10")
    deleted = await storage.delete_bookmark(uid, "tmp")
    assert deleted is True
    assert await storage.get_bookmark(uid, "tmp") is None


@pytest.mark.asyncio
async def test_delete_bookmark_missing():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    deleted = await storage.delete_bookmark(uid, "ghost")
    assert deleted is False


@pytest.mark.asyncio
async def test_multiple_bookmarks_ordered_by_name():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    await storage.upsert_bookmark(uid, "z-end", "page:10")
    await storage.upsert_bookmark(uid, "a-intro", "page:1")
    await storage.upsert_bookmark(uid, "m-middle", "page:5")
    bms = await storage.get_bookmarks(uid)
    assert [b["name"] for b in bms] == ["a-intro", "m-middle", "z-end"]


# ---------------------------------------------------------------------------
# API endpoint tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_upsert_bookmark_200():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.put(f"/results/{doc_id}/bookmarks/intro",
                        json={"name": "intro", "reference": "page:1"})
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_upsert_bookmark_response_structure():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.put(f"/results/{doc_id}/bookmarks/intro",
                        json={"name": "intro", "reference": "page:1"})
    body = r.json()
    assert body["name"] == "intro"
    assert body["reference"] == "page:1"
    assert "id" in body
    assert "created_at" in body


@pytest.mark.asyncio
async def test_upsert_bookmark_404_missing_result():
    async with _client() as c:
        r = await c.put(f"/results/{uuid4()}/bookmarks/intro",
                        json={"name": "intro", "reference": "page:1"})
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_list_bookmarks_200():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.get(f"/results/{doc_id}/bookmarks")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_list_bookmarks_empty():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.get(f"/results/{doc_id}/bookmarks")
    assert r.json()["bookmarks"] == []


@pytest.mark.asyncio
async def test_list_bookmarks_after_upsert():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.put(f"/results/{doc_id}/bookmarks/sec1",
                    json={"name": "sec1", "reference": "section:1"})
        await c.put(f"/results/{doc_id}/bookmarks/sec2",
                    json={"name": "sec2", "reference": "section:2"})
        r = await c.get(f"/results/{doc_id}/bookmarks")
    assert len(r.json()["bookmarks"]) == 2


@pytest.mark.asyncio
async def test_get_single_bookmark_200():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.put(f"/results/{doc_id}/bookmarks/intro",
                    json={"name": "intro", "reference": "page:1"})
        r = await c.get(f"/results/{doc_id}/bookmarks/intro")
    assert r.status_code == 200
    assert r.json()["name"] == "intro"


@pytest.mark.asyncio
async def test_get_single_bookmark_404():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.get(f"/results/{doc_id}/bookmarks/ghost")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_delete_bookmark_204():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.put(f"/results/{doc_id}/bookmarks/tmp",
                    json={"name": "tmp", "reference": "line:1"})
        r = await c.delete(f"/results/{doc_id}/bookmarks/tmp")
    assert r.status_code == 204


@pytest.mark.asyncio
async def test_delete_bookmark_404():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.delete(f"/results/{doc_id}/bookmarks/ghost")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_upsert_is_idempotent():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.put(f"/results/{doc_id}/bookmarks/intro",
                    json={"name": "intro", "reference": "page:1"})
        await c.put(f"/results/{doc_id}/bookmarks/intro",
                    json={"name": "intro", "reference": "page:2"})
        r = await c.get(f"/results/{doc_id}/bookmarks")
    assert len(r.json()["bookmarks"]) == 1
    assert r.json()["bookmarks"][0]["reference"] == "page:2"


@pytest.mark.asyncio
async def test_bookmark_set_recorded_in_audit():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.put(f"/results/{doc_id}/bookmarks/key",
                    json={"name": "key", "reference": "line:42"})
        r = await c.get(f"/results/{doc_id}/audit")
    actions = [e["action"] for e in r.json()["entries"]]
    assert "bookmark_set" in actions


@pytest.mark.asyncio
async def test_bookmark_deleted_recorded_in_audit():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.put(f"/results/{doc_id}/bookmarks/key",
                    json={"name": "key", "reference": "line:42"})
        await c.delete(f"/results/{doc_id}/bookmarks/key")
        r = await c.get(f"/results/{doc_id}/audit")
    actions = [e["action"] for e in r.json()["entries"]]
    assert "bookmark_deleted" in actions
