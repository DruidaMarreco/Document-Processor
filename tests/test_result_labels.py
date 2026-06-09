"""Tests for label registry and per-result label assignment."""
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
async def test_create_label_storage():
    label = await storage.create_label("urgent", "#ff0000", "High priority items")
    assert label["name"] == "urgent"
    assert label["color"] == "#ff0000"


@pytest.mark.asyncio
async def test_list_labels_empty():
    labels = await storage.list_labels()
    assert labels == []


@pytest.mark.asyncio
async def test_list_labels_after_create():
    await storage.create_label("bug", "#cc0000")
    await storage.create_label("feature", "#00cc00")
    labels = await storage.list_labels()
    assert len(labels) == 2
    names = [l["name"] for l in labels]
    assert "bug" in names
    assert "feature" in names


@pytest.mark.asyncio
async def test_get_label():
    await storage.create_label("urgent", "#ff0000")
    label = await storage.get_label("urgent")
    assert label is not None
    assert label["name"] == "urgent"


@pytest.mark.asyncio
async def test_get_label_missing():
    label = await storage.get_label("nonexistent")
    assert label is None


@pytest.mark.asyncio
async def test_delete_label():
    await storage.create_label("temp", "#aaaaaa")
    deleted = await storage.delete_label("temp")
    assert deleted is True
    assert await storage.get_label("temp") is None


@pytest.mark.asyncio
async def test_delete_label_missing():
    deleted = await storage.delete_label("ghost")
    assert deleted is False


@pytest.mark.asyncio
async def test_apply_and_get_result_labels():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    await storage.create_label("approved", "#00ff00")
    await storage.apply_label(uid, "approved")
    labels = await storage.get_result_labels(uid)
    assert len(labels) == 1
    assert labels[0]["name"] == "approved"


@pytest.mark.asyncio
async def test_apply_label_idempotent():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    await storage.create_label("reviewed", "#0000ff")
    await storage.apply_label(uid, "reviewed")
    await storage.apply_label(uid, "reviewed")
    labels = await storage.get_result_labels(uid)
    assert len(labels) == 1


@pytest.mark.asyncio
async def test_remove_label_from_result():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    await storage.create_label("done", "#888888")
    await storage.apply_label(uid, "done")
    removed = await storage.remove_label_from_result(uid, "done")
    assert removed is True
    assert await storage.get_result_labels(uid) == []


@pytest.mark.asyncio
async def test_delete_label_cascades_to_results():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    await storage.create_label("cascade", "#111111")
    await storage.apply_label(uid, "cascade")
    await storage.delete_label("cascade")
    assert await storage.get_result_labels(uid) == []


# ---------------------------------------------------------------------------
# API endpoint tests — label registry
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_create_label_201():
    async with _client() as c:
        r = await c.post("/labels", json={"name": "urgent", "color": "#ff0000"})
    assert r.status_code == 201


@pytest.mark.asyncio
async def test_create_label_409_duplicate():
    async with _client() as c:
        await c.post("/labels", json={"name": "dup"})
        r = await c.post("/labels", json={"name": "dup"})
    assert r.status_code == 409


@pytest.mark.asyncio
async def test_list_labels_endpoint():
    async with _client() as c:
        await c.post("/labels", json={"name": "a"})
        await c.post("/labels", json={"name": "b"})
        r = await c.get("/labels")
    assert r.status_code == 200
    assert len(r.json()["labels"]) == 2


@pytest.mark.asyncio
async def test_get_label_endpoint():
    async with _client() as c:
        await c.post("/labels", json={"name": "hotfix", "color": "#ff9900"})
        r = await c.get("/labels/hotfix")
    assert r.status_code == 200
    assert r.json()["name"] == "hotfix"


@pytest.mark.asyncio
async def test_get_label_404():
    async with _client() as c:
        r = await c.get("/labels/ghost")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_delete_label_endpoint():
    async with _client() as c:
        await c.post("/labels", json={"name": "temp"})
        r = await c.delete("/labels/temp")
    assert r.status_code == 204


@pytest.mark.asyncio
async def test_delete_label_404():
    async with _client() as c:
        r = await c.delete("/labels/ghost")
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# API endpoint tests — per-result label assignment
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_apply_label_to_result_204():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.post("/labels", json={"name": "reviewed"})
        r = await c.post(f"/results/{doc_id}/labels", json={"label": "reviewed"})
    assert r.status_code == 204


@pytest.mark.asyncio
async def test_apply_label_404_missing_result():
    async with _client() as c:
        await c.post("/labels", json={"name": "reviewed"})
        r = await c.post(f"/results/{uuid4()}/labels", json={"label": "reviewed"})
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_apply_label_404_missing_label():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.post(f"/results/{doc_id}/labels", json={"label": "ghost"})
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_get_result_labels_empty():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.get(f"/results/{doc_id}/labels")
    assert r.status_code == 200
    assert r.json()["labels"] == []


@pytest.mark.asyncio
async def test_get_result_labels_after_apply():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.post("/labels", json={"name": "urgent", "color": "#ff0000"})
        await c.post(f"/results/{doc_id}/labels", json={"label": "urgent"})
        r = await c.get(f"/results/{doc_id}/labels")
    labels = r.json()["labels"]
    assert len(labels) == 1
    assert labels[0]["name"] == "urgent"
    assert labels[0]["color"] == "#ff0000"


@pytest.mark.asyncio
async def test_remove_label_from_result_204():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.post("/labels", json={"name": "done"})
        await c.post(f"/results/{doc_id}/labels", json={"label": "done"})
        r = await c.delete(f"/results/{doc_id}/labels/done")
    assert r.status_code == 204


@pytest.mark.asyncio
async def test_remove_label_404_not_assigned():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.post("/labels", json={"name": "x"})
        r = await c.delete(f"/results/{doc_id}/labels/x")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_list_results_by_label():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL, "email.txt")
        await c.post("/labels", json={"name": "vip"})
        await c.post(f"/results/{id1}/labels", json={"label": "vip"})
        r = await c.get("/results?label=vip")
    body = r.json()
    assert body["total"] == 1
    assert body["results"][0]["document_id"] == id1


@pytest.mark.asyncio
async def test_apply_label_recorded_in_audit():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.post("/labels", json={"name": "checked"})
        await c.post(f"/results/{doc_id}/labels", json={"label": "checked"})
        r = await c.get(f"/results/{doc_id}/audit")
    actions = [e["action"] for e in r.json()["entries"]]
    assert "label_applied" in actions
