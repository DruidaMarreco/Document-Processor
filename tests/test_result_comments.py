"""Tests for threaded result comments — POST/GET/DELETE /results/{id}/comments."""
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
    r = await client.post("/process", files={"file": ("inv.txt", _INVOICE, "text/plain")})
    assert r.status_code == 200
    return r.json()["document_id"]


# ---------------------------------------------------------------------------
# Storage unit tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_add_and_retrieve_comment():
    from uuid import uuid4 as _uuid4
    # create a result first
    async with _client() as c:
        doc_id = await _upload(c)

    comment = await storage.add_comment(__import__("uuid").UUID(doc_id), "First comment")
    assert comment["text"] == "First comment"
    assert comment["result_id"] == doc_id
    assert "id" in comment
    assert "created_at" in comment


@pytest.mark.asyncio
async def test_multiple_comments_per_result():
    async with _client() as c:
        doc_id = await _upload(c)

    uid = __import__("uuid").UUID(doc_id)
    await storage.add_comment(uid, "Comment A")
    await storage.add_comment(uid, "Comment B")
    comments, total = await storage.get_comments(uid)
    assert total == 2
    texts = [c["text"] for c in comments]
    assert "Comment A" in texts
    assert "Comment B" in texts


@pytest.mark.asyncio
async def test_comments_isolated_by_result():
    async with _client() as c:
        doc1 = await _upload(c)
        doc2 = await _upload(c)

    uid1 = __import__("uuid").UUID(doc1)
    uid2 = __import__("uuid").UUID(doc2)
    await storage.add_comment(uid1, "For doc1")
    comments, total = await storage.get_comments(uid2)
    assert total == 0


@pytest.mark.asyncio
async def test_delete_comment():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    comment = await storage.add_comment(uid, "To delete")
    removed = await storage.delete_comment(uid, comment["id"])
    assert removed is True
    _, total = await storage.get_comments(uid)
    assert total == 0


@pytest.mark.asyncio
async def test_delete_nonexistent_comment_returns_false():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    removed = await storage.delete_comment(uid, 99999)
    assert removed is False


# ---------------------------------------------------------------------------
# API endpoint tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_add_comment_201():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.post(f"/results/{doc_id}/comments", json={"text": "Hello"})
    assert r.status_code == 201


@pytest.mark.asyncio
async def test_add_comment_response_structure():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.post(f"/results/{doc_id}/comments", json={"text": "Check structure"})
    body = r.json()
    assert "id" in body
    assert "text" in body
    assert "result_id" in body
    assert "created_at" in body
    assert body["text"] == "Check structure"
    assert body["result_id"] == doc_id


@pytest.mark.asyncio
async def test_add_comment_404_for_missing_result():
    async with _client() as c:
        r = await c.post(f"/results/{uuid4()}/comments", json={"text": "Hi"})
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_add_empty_comment_returns_400():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.post(f"/results/{doc_id}/comments", json={"text": "   "})
    assert r.status_code == 400


@pytest.mark.asyncio
async def test_list_comments_empty():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.get(f"/results/{doc_id}/comments")
    assert r.status_code == 200
    body = r.json()
    assert body["total"] == 0
    assert body["comments"] == []


@pytest.mark.asyncio
async def test_list_comments_response_structure():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.post(f"/results/{doc_id}/comments", json={"text": "Test"})
        r = await c.get(f"/results/{doc_id}/comments")
    body = r.json()
    assert "document_id" in body
    assert "total" in body
    assert "offset" in body
    assert "limit" in body
    assert "comments" in body
    assert body["document_id"] == doc_id


@pytest.mark.asyncio
async def test_list_comments_404_for_missing_result():
    async with _client() as c:
        r = await c.get(f"/results/{uuid4()}/comments")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_list_comments_ordered_ascending():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.post(f"/results/{doc_id}/comments", json={"text": "First"})
        await c.post(f"/results/{doc_id}/comments", json={"text": "Second"})
        r = await c.get(f"/results/{doc_id}/comments")
    texts = [c["text"] for c in r.json()["comments"]]
    assert texts == ["First", "Second"]


@pytest.mark.asyncio
async def test_delete_comment_204():
    async with _client() as c:
        doc_id = await _upload(c)
        r_add = await c.post(f"/results/{doc_id}/comments", json={"text": "Delete me"})
        cid = r_add.json()["id"]
        r_del = await c.delete(f"/results/{doc_id}/comments/{cid}")
    assert r_del.status_code == 204


@pytest.mark.asyncio
async def test_delete_comment_removes_it():
    async with _client() as c:
        doc_id = await _upload(c)
        r_add = await c.post(f"/results/{doc_id}/comments", json={"text": "Gone"})
        cid = r_add.json()["id"]
        await c.delete(f"/results/{doc_id}/comments/{cid}")
        r_list = await c.get(f"/results/{doc_id}/comments")
    assert r_list.json()["total"] == 0


@pytest.mark.asyncio
async def test_delete_comment_404_for_missing():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.delete(f"/results/{doc_id}/comments/99999")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_delete_comment_404_for_missing_result():
    async with _client() as c:
        r = await c.delete(f"/results/{uuid4()}/comments/1")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_comments_pagination():
    async with _client() as c:
        doc_id = await _upload(c)
        for i in range(8):
            await c.post(f"/results/{doc_id}/comments", json={"text": f"msg {i}"})
        r = await c.get(f"/results/{doc_id}/comments?limit=3&offset=0")
    body = r.json()
    assert body["total"] == 8
    assert len(body["comments"]) == 3


@pytest.mark.asyncio
async def test_comment_added_to_audit_log():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.post(f"/results/{doc_id}/comments", json={"text": "Audit me"})
        r = await c.get(f"/results/{doc_id}/audit")
    actions = [e["action"] for e in r.json()["entries"]]
    assert "comment_added" in actions
