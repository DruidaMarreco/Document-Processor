"""Tests for result snapshots — POST/GET /results/{id}/snapshots."""
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


async def _upload(client, content: bytes = _INVOICE) -> str:
    r = await client.post("/process", files={"file": ("doc.txt", content, "text/plain")})
    assert r.status_code == 200
    return r.json()["document_id"]


# ---------------------------------------------------------------------------
# Storage unit tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_create_snapshot_returns_none_for_missing():
    result = await storage.create_snapshot(uuid4())
    assert result is None


@pytest.mark.asyncio
async def test_create_snapshot_returns_dict():
    async with _client() as c:
        doc_id = await _upload(c)
    snap = await storage.create_snapshot(__import__("uuid").UUID(doc_id))
    assert snap is not None
    assert snap["result_id"] == doc_id
    assert "id" in snap
    assert "created_at" in snap


@pytest.mark.asyncio
async def test_create_snapshot_with_label():
    async with _client() as c:
        doc_id = await _upload(c)
    snap = await storage.create_snapshot(__import__("uuid").UUID(doc_id), label="before reprocess")
    assert snap["label"] == "before reprocess"


@pytest.mark.asyncio
async def test_create_snapshot_without_label():
    async with _client() as c:
        doc_id = await _upload(c)
    snap = await storage.create_snapshot(__import__("uuid").UUID(doc_id))
    assert snap["label"] is None


@pytest.mark.asyncio
async def test_list_snapshots_empty():
    async with _client() as c:
        doc_id = await _upload(c)
    snaps = await storage.list_snapshots(__import__("uuid").UUID(doc_id))
    assert snaps == []


@pytest.mark.asyncio
async def test_list_snapshots_after_create():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    await storage.create_snapshot(uid, label="v1")
    await storage.create_snapshot(uid, label="v2")
    snaps = await storage.list_snapshots(uid)
    assert len(snaps) == 2


@pytest.mark.asyncio
async def test_list_snapshots_newest_first():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    s1 = await storage.create_snapshot(uid, label="first")
    s2 = await storage.create_snapshot(uid, label="second")
    snaps = await storage.list_snapshots(uid)
    assert snaps[0]["id"] == s2["id"]
    assert snaps[1]["id"] == s1["id"]


@pytest.mark.asyncio
async def test_get_snapshot_returns_data():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    snap = await storage.create_snapshot(uid)
    full = await storage.get_snapshot(uid, snap["id"])
    assert full is not None
    assert "data" in full
    assert isinstance(full["data"], dict)
    assert "document_id" in full["data"]


@pytest.mark.asyncio
async def test_get_snapshot_wrong_result_returns_none():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL)
    uid1 = __import__("uuid").UUID(id1)
    uid2 = __import__("uuid").UUID(id2)
    snap = await storage.create_snapshot(uid1)
    result = await storage.get_snapshot(uid2, snap["id"])
    assert result is None


@pytest.mark.asyncio
async def test_get_snapshot_not_found():
    async with _client() as c:
        doc_id = await _upload(c)
    result = await storage.get_snapshot(__import__("uuid").UUID(doc_id), 99999)
    assert result is None


# ---------------------------------------------------------------------------
# API endpoint tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_create_snapshot_endpoint_201():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.post(f"/results/{doc_id}/snapshots")
    assert r.status_code == 201


@pytest.mark.asyncio
async def test_create_snapshot_response_structure():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.post(f"/results/{doc_id}/snapshots")
    body = r.json()
    assert "id" in body
    assert body["result_id"] == doc_id
    assert "created_at" in body


@pytest.mark.asyncio
async def test_create_snapshot_with_label_endpoint():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.post(f"/results/{doc_id}/snapshots", json={"label": "checkpoint"})
    assert r.json()["label"] == "checkpoint"


@pytest.mark.asyncio
async def test_create_snapshot_404_missing():
    async with _client() as c:
        r = await c.post(f"/results/{uuid4()}/snapshots")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_list_snapshots_endpoint_200():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.get(f"/results/{doc_id}/snapshots")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_list_snapshots_response_structure():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.get(f"/results/{doc_id}/snapshots")
    body = r.json()
    assert body["document_id"] == doc_id
    assert isinstance(body["snapshots"], list)


@pytest.mark.asyncio
async def test_list_snapshots_empty_initially():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.get(f"/results/{doc_id}/snapshots")
    assert r.json()["snapshots"] == []


@pytest.mark.asyncio
async def test_list_snapshots_populated_after_create():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.post(f"/results/{doc_id}/snapshots", json={"label": "v1"})
        await c.post(f"/results/{doc_id}/snapshots", json={"label": "v2"})
        r = await c.get(f"/results/{doc_id}/snapshots")
    assert len(r.json()["snapshots"]) == 2


@pytest.mark.asyncio
async def test_list_snapshots_404_missing():
    async with _client() as c:
        r = await c.get(f"/results/{uuid4()}/snapshots")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_get_snapshot_endpoint_200():
    async with _client() as c:
        doc_id = await _upload(c)
        r_create = await c.post(f"/results/{doc_id}/snapshots")
        snap_id = r_create.json()["id"]
        r = await c.get(f"/results/{doc_id}/snapshots/{snap_id}")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_get_snapshot_includes_data():
    async with _client() as c:
        doc_id = await _upload(c)
        r_create = await c.post(f"/results/{doc_id}/snapshots")
        snap_id = r_create.json()["id"]
        r = await c.get(f"/results/{doc_id}/snapshots/{snap_id}")
    body = r.json()
    assert "data" in body
    assert isinstance(body["data"], dict)


@pytest.mark.asyncio
async def test_get_snapshot_404_missing():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.get(f"/results/{doc_id}/snapshots/99999")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_snapshot_in_audit_log():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.post(f"/results/{doc_id}/snapshots")
        r = await c.get(f"/results/{doc_id}/audit")
    actions = [e["action"] for e in r.json()["entries"]]
    assert "snapshot_created" in actions
