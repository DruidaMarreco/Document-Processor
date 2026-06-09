"""Tests for result file attachments — POST/GET/DELETE /results/{id}/attachments."""
from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient

from document_processor import storage
from document_processor.api import app

_INVOICE = b"Invoice No: 1234\nBill To: John\nAmount Due: $500\nDue Date: 2026-01-01"
_ATTACHMENT = b"This is a test attachment file."


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
async def test_attachments_empty_initially():
    async with _client() as c:
        doc_id = await _upload(c)
    items = await storage.list_attachments(__import__("uuid").UUID(doc_id))
    assert items == []


@pytest.mark.asyncio
async def test_save_attachment_returns_metadata():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    att = await storage.save_attachment(uid, "report.txt", "text/plain", _ATTACHMENT)
    assert att["filename"] == "report.txt"
    assert att["mimetype"] == "text/plain"
    assert att["size"] == len(_ATTACHMENT)
    assert "id" in att
    assert "created_at" in att


@pytest.mark.asyncio
async def test_list_attachments_returns_metadata_only():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    await storage.save_attachment(uid, "a.txt", "text/plain", _ATTACHMENT)
    items = await storage.list_attachments(uid)
    assert len(items) == 1
    assert "content" not in items[0]


@pytest.mark.asyncio
async def test_get_attachment_includes_content():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    saved = await storage.save_attachment(uid, "a.txt", "text/plain", _ATTACHMENT)
    fetched = await storage.get_attachment(uid, saved["id"])
    assert fetched is not None
    assert fetched["content"] == _ATTACHMENT


@pytest.mark.asyncio
async def test_get_attachment_missing_returns_none():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    assert await storage.get_attachment(uid, 9999) is None


@pytest.mark.asyncio
async def test_delete_attachment():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    saved = await storage.save_attachment(uid, "del.txt", "text/plain", _ATTACHMENT)
    deleted = await storage.delete_attachment(uid, saved["id"])
    assert deleted is True
    assert await storage.get_attachment(uid, saved["id"]) is None


@pytest.mark.asyncio
async def test_delete_attachment_missing_returns_false():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    assert await storage.delete_attachment(uid, 9999) is False


@pytest.mark.asyncio
async def test_multiple_attachments_ordered_by_created_at():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    await storage.save_attachment(uid, "first.txt", "text/plain", b"a")
    await storage.save_attachment(uid, "second.txt", "text/plain", b"b")
    items = await storage.list_attachments(uid)
    assert len(items) == 2
    assert items[0]["filename"] == "first.txt"
    assert items[1]["filename"] == "second.txt"


# ---------------------------------------------------------------------------
# API endpoint tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_upload_attachment_201():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.post(
            f"/results/{doc_id}/attachments",
            files={"file": ("report.txt", _ATTACHMENT, "text/plain")},
        )
    assert r.status_code == 201


@pytest.mark.asyncio
async def test_upload_attachment_response_structure():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.post(
            f"/results/{doc_id}/attachments",
            files={"file": ("report.txt", _ATTACHMENT, "text/plain")},
        )
    body = r.json()
    assert "id" in body
    assert body["filename"] == "report.txt"
    assert "size" in body
    assert "created_at" in body


@pytest.mark.asyncio
async def test_upload_attachment_404_missing_result():
    async with _client() as c:
        r = await c.post(
            f"/results/{uuid4()}/attachments",
            files={"file": ("x.txt", _ATTACHMENT, "text/plain")},
        )
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_list_attachments_200():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.get(f"/results/{doc_id}/attachments")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_list_attachments_empty():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.get(f"/results/{doc_id}/attachments")
    assert r.json()["attachments"] == []


@pytest.mark.asyncio
async def test_list_attachments_after_upload():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.post(
            f"/results/{doc_id}/attachments",
            files={"file": ("a.txt", b"aaa", "text/plain")},
        )
        await c.post(
            f"/results/{doc_id}/attachments",
            files={"file": ("b.txt", b"bbb", "text/plain")},
        )
        r = await c.get(f"/results/{doc_id}/attachments")
    assert len(r.json()["attachments"]) == 2


@pytest.mark.asyncio
async def test_list_attachments_404_missing_result():
    async with _client() as c:
        r = await c.get(f"/results/{uuid4()}/attachments")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_download_attachment_200():
    async with _client() as c:
        doc_id = await _upload(c)
        att = (await c.post(
            f"/results/{doc_id}/attachments",
            files={"file": ("report.txt", _ATTACHMENT, "text/plain")},
        )).json()
        r = await c.get(f"/results/{doc_id}/attachments/{att['id']}")
    assert r.status_code == 200
    assert r.content == _ATTACHMENT


@pytest.mark.asyncio
async def test_download_attachment_content_type():
    async with _client() as c:
        doc_id = await _upload(c)
        att = (await c.post(
            f"/results/{doc_id}/attachments",
            files={"file": ("report.txt", _ATTACHMENT, "text/plain")},
        )).json()
        r = await c.get(f"/results/{doc_id}/attachments/{att['id']}")
    assert "text/plain" in r.headers["content-type"]


@pytest.mark.asyncio
async def test_download_attachment_404():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.get(f"/results/{doc_id}/attachments/9999")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_delete_attachment_204():
    async with _client() as c:
        doc_id = await _upload(c)
        att = (await c.post(
            f"/results/{doc_id}/attachments",
            files={"file": ("del.txt", _ATTACHMENT, "text/plain")},
        )).json()
        r = await c.delete(f"/results/{doc_id}/attachments/{att['id']}")
    assert r.status_code == 204


@pytest.mark.asyncio
async def test_delete_attachment_404():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.delete(f"/results/{doc_id}/attachments/9999")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_attachment_upload_recorded_in_audit():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.post(
            f"/results/{doc_id}/attachments",
            files={"file": ("audit.txt", _ATTACHMENT, "text/plain")},
        )
        r = await c.get(f"/results/{doc_id}/audit")
    actions = [e["action"] for e in r.json()["entries"]]
    assert "attachment_added" in actions


@pytest.mark.asyncio
async def test_attachment_delete_recorded_in_audit():
    async with _client() as c:
        doc_id = await _upload(c)
        att = (await c.post(
            f"/results/{doc_id}/attachments",
            files={"file": ("del.txt", _ATTACHMENT, "text/plain")},
        )).json()
        await c.delete(f"/results/{doc_id}/attachments/{att['id']}")
        r = await c.get(f"/results/{doc_id}/audit")
    actions = [e["action"] for e in r.json()["entries"]]
    assert "attachment_deleted" in actions
