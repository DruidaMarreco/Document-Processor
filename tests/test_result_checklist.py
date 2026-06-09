"""Tests for result checklist — POST/GET/PUT/DELETE /results/{id}/checklist."""
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
async def test_checklist_empty_initially():
    async with _client() as c:
        doc_id = await _upload(c)
    items = await storage.get_checklist(__import__("uuid").UUID(doc_id))
    assert items == []


@pytest.mark.asyncio
async def test_add_checklist_item():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    item = await storage.add_checklist_item(uid, "Review extracted fields")
    assert item["text"] == "Review extracted fields"
    assert item["checked"] is False
    assert "id" in item


@pytest.mark.asyncio
async def test_checklist_items_ordered_by_position():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    await storage.add_checklist_item(uid, "First")
    await storage.add_checklist_item(uid, "Second")
    await storage.add_checklist_item(uid, "Third")
    items = await storage.get_checklist(uid)
    assert [i["text"] for i in items] == ["First", "Second", "Third"]


@pytest.mark.asyncio
async def test_set_checklist_item_checked():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    item = await storage.add_checklist_item(uid, "Do this")
    updated = await storage.set_checklist_item_checked(uid, item["id"], True)
    assert updated is True
    items = await storage.get_checklist(uid)
    assert items[0]["checked"] is True


@pytest.mark.asyncio
async def test_set_checklist_item_unchecked():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    item = await storage.add_checklist_item(uid, "Do this")
    await storage.set_checklist_item_checked(uid, item["id"], True)
    await storage.set_checklist_item_checked(uid, item["id"], False)
    items = await storage.get_checklist(uid)
    assert items[0]["checked"] is False


@pytest.mark.asyncio
async def test_set_checked_returns_false_for_missing():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    updated = await storage.set_checklist_item_checked(uid, 9999, True)
    assert updated is False


@pytest.mark.asyncio
async def test_delete_checklist_item():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    item = await storage.add_checklist_item(uid, "Delete me")
    deleted = await storage.delete_checklist_item(uid, item["id"])
    assert deleted is True
    assert await storage.get_checklist(uid) == []


@pytest.mark.asyncio
async def test_checklist_progress():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    i1 = await storage.add_checklist_item(uid, "A")
    i2 = await storage.add_checklist_item(uid, "B")
    await storage.add_checklist_item(uid, "C")
    await storage.set_checklist_item_checked(uid, i1["id"], True)
    await storage.set_checklist_item_checked(uid, i2["id"], True)
    prog = await storage.get_checklist_progress(uid)
    assert prog["total"] == 3
    assert prog["checked"] == 2
    assert prog["unchecked"] == 1


# ---------------------------------------------------------------------------
# API endpoint tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_add_checklist_item_201():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.post(f"/results/{doc_id}/checklist", json={"text": "Check this"})
    assert r.status_code == 201


@pytest.mark.asyncio
async def test_add_checklist_item_404_missing():
    async with _client() as c:
        r = await c.post(f"/results/{uuid4()}/checklist", json={"text": "Check this"})
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_get_checklist_200():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.get(f"/results/{doc_id}/checklist")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_get_checklist_response_structure():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.get(f"/results/{doc_id}/checklist")
    body = r.json()
    assert "document_id" in body
    assert "items" in body
    assert "progress" in body


@pytest.mark.asyncio
async def test_get_checklist_empty():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.get(f"/results/{doc_id}/checklist")
    assert r.json()["items"] == []


@pytest.mark.asyncio
async def test_get_checklist_with_items():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.post(f"/results/{doc_id}/checklist", json={"text": "Step 1"})
        await c.post(f"/results/{doc_id}/checklist", json={"text": "Step 2"})
        r = await c.get(f"/results/{doc_id}/checklist")
    assert len(r.json()["items"]) == 2


@pytest.mark.asyncio
async def test_check_item_204():
    async with _client() as c:
        doc_id = await _upload(c)
        item = (await c.post(f"/results/{doc_id}/checklist", json={"text": "Do it"})).json()
        r = await c.put(f"/results/{doc_id}/checklist/{item['id']}/check")
    assert r.status_code == 204


@pytest.mark.asyncio
async def test_uncheck_item_204():
    async with _client() as c:
        doc_id = await _upload(c)
        item = (await c.post(f"/results/{doc_id}/checklist", json={"text": "Do it"})).json()
        await c.put(f"/results/{doc_id}/checklist/{item['id']}/check")
        r = await c.put(f"/results/{doc_id}/checklist/{item['id']}/uncheck")
    assert r.status_code == 204


@pytest.mark.asyncio
async def test_check_item_404():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.put(f"/results/{doc_id}/checklist/9999/check")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_delete_checklist_item_204():
    async with _client() as c:
        doc_id = await _upload(c)
        item = (await c.post(f"/results/{doc_id}/checklist", json={"text": "Remove me"})).json()
        r = await c.delete(f"/results/{doc_id}/checklist/{item['id']}")
    assert r.status_code == 204


@pytest.mark.asyncio
async def test_delete_checklist_item_404():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.delete(f"/results/{doc_id}/checklist/9999")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_progress_endpoint():
    async with _client() as c:
        doc_id = await _upload(c)
        item = (await c.post(f"/results/{doc_id}/checklist", json={"text": "A"})).json()
        await c.post(f"/results/{doc_id}/checklist", json={"text": "B"})
        await c.put(f"/results/{doc_id}/checklist/{item['id']}/check")
        r = await c.get(f"/results/{doc_id}/checklist/progress")
    body = r.json()
    assert body["total"] == 2
    assert body["checked"] == 1
    assert body["unchecked"] == 1


@pytest.mark.asyncio
async def test_checklist_item_added_recorded_in_audit():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.post(f"/results/{doc_id}/checklist", json={"text": "Audit me"})
        r = await c.get(f"/results/{doc_id}/audit")
    actions = [e["action"] for e in r.json()["entries"]]
    assert "checklist_item_added" in actions
