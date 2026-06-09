"""Tests for custom key-value metadata — GET/PUT/DELETE /results/{id}/metadata[/{key}]."""
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
async def test_set_and_get_metadata():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    await storage.set_metadata(uid, "priority", 1)
    entry = await storage.get_metadata_key(uid, "priority")
    assert entry is not None
    assert entry["value"] == 1
    assert entry["key"] == "priority"


@pytest.mark.asyncio
async def test_metadata_supports_various_types():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    await storage.set_metadata(uid, "str_val", "hello")
    await storage.set_metadata(uid, "int_val", 42)
    await storage.set_metadata(uid, "float_val", 3.14)
    await storage.set_metadata(uid, "bool_val", True)
    await storage.set_metadata(uid, "list_val", [1, 2, 3])
    await storage.set_metadata(uid, "dict_val", {"a": 1})
    meta = await storage.get_metadata(uid)
    assert meta["str_val"]["value"] == "hello"
    assert meta["int_val"]["value"] == 42
    assert meta["float_val"]["value"] == 3.14
    assert meta["bool_val"]["value"] is True
    assert meta["list_val"]["value"] == [1, 2, 3]
    assert meta["dict_val"]["value"] == {"a": 1}


@pytest.mark.asyncio
async def test_set_metadata_overwrites_existing():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    await storage.set_metadata(uid, "k", "old")
    await storage.set_metadata(uid, "k", "new")
    entry = await storage.get_metadata_key(uid, "k")
    assert entry["value"] == "new"


@pytest.mark.asyncio
async def test_get_metadata_returns_all_keys():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    await storage.set_metadata(uid, "a", 1)
    await storage.set_metadata(uid, "b", 2)
    await storage.set_metadata(uid, "c", 3)
    meta = await storage.get_metadata(uid)
    assert set(meta.keys()) == {"a", "b", "c"}


@pytest.mark.asyncio
async def test_get_metadata_empty_for_new_result():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    meta = await storage.get_metadata(uid)
    assert meta == {}


@pytest.mark.asyncio
async def test_get_metadata_key_missing_returns_none():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    entry = await storage.get_metadata_key(uid, "nonexistent")
    assert entry is None


@pytest.mark.asyncio
async def test_delete_metadata_key():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    await storage.set_metadata(uid, "x", 99)
    removed = await storage.delete_metadata_key(uid, "x")
    assert removed is True
    entry = await storage.get_metadata_key(uid, "x")
    assert entry is None


@pytest.mark.asyncio
async def test_delete_missing_metadata_key_returns_false():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    removed = await storage.delete_metadata_key(uid, "missing")
    assert removed is False


@pytest.mark.asyncio
async def test_metadata_isolated_between_results():
    async with _client() as c:
        id1 = await _upload(c)
        id2 = await _upload(c)
    await storage.set_metadata(__import__("uuid").UUID(id1), "k", "v1")
    meta2 = await storage.get_metadata(__import__("uuid").UUID(id2))
    assert "k" not in meta2


# ---------------------------------------------------------------------------
# API endpoint tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_put_metadata_204():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.put(f"/results/{doc_id}/metadata/priority",
                        json={"value": 5})
    assert r.status_code == 204


@pytest.mark.asyncio
async def test_put_metadata_404_for_missing_result():
    async with _client() as c:
        r = await c.put(f"/results/{uuid4()}/metadata/k", json={"value": 1})
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_get_metadata_empty():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.get(f"/results/{doc_id}/metadata")
    assert r.status_code == 200
    body = r.json()
    assert body["document_id"] == doc_id
    assert body["metadata"] == {}


@pytest.mark.asyncio
async def test_get_metadata_shows_set_keys():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.put(f"/results/{doc_id}/metadata/score", json={"value": 0.95})
        await c.put(f"/results/{doc_id}/metadata/reviewed", json={"value": True})
        r = await c.get(f"/results/{doc_id}/metadata")
    meta = r.json()["metadata"]
    assert "score" in meta
    assert "reviewed" in meta
    assert meta["score"]["value"] == 0.95
    assert meta["reviewed"]["value"] is True


@pytest.mark.asyncio
async def test_get_metadata_404_for_missing_result():
    async with _client() as c:
        r = await c.get(f"/results/{uuid4()}/metadata")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_get_single_metadata_key():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.put(f"/results/{doc_id}/metadata/notes", json={"value": "check this"})
        r = await c.get(f"/results/{doc_id}/metadata/notes")
    assert r.status_code == 200
    body = r.json()
    assert body["key"] == "notes"
    assert body["value"] == "check this"
    assert "updated_at" in body


@pytest.mark.asyncio
async def test_get_single_metadata_key_404_missing_key():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.get(f"/results/{doc_id}/metadata/nonexistent")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_get_single_metadata_key_404_missing_result():
    async with _client() as c:
        r = await c.get(f"/results/{uuid4()}/metadata/k")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_delete_metadata_key_204():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.put(f"/results/{doc_id}/metadata/tmp", json={"value": "bye"})
        r = await c.delete(f"/results/{doc_id}/metadata/tmp")
    assert r.status_code == 204


@pytest.mark.asyncio
async def test_delete_metadata_key_removes_it():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.put(f"/results/{doc_id}/metadata/gone", json={"value": 1})
        await c.delete(f"/results/{doc_id}/metadata/gone")
        r = await c.get(f"/results/{doc_id}/metadata")
    assert "gone" not in r.json()["metadata"]


@pytest.mark.asyncio
async def test_delete_metadata_key_404_missing():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.delete(f"/results/{doc_id}/metadata/missing")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_delete_metadata_key_404_missing_result():
    async with _client() as c:
        r = await c.delete(f"/results/{uuid4()}/metadata/k")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_metadata_set_in_audit_log():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.put(f"/results/{doc_id}/metadata/source", json={"value": "crm"})
        r = await c.get(f"/results/{doc_id}/audit")
    actions = [e["action"] for e in r.json()["entries"]]
    assert "metadata_set" in actions
