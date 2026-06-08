"""Tests for result retention/cleanup and document notes."""
from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import aiosqlite
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


# ---------------------------------------------------------------------------
# Retention / cleanup
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cleanup_zero_days_deletes_nothing():
    await storage.init_db()
    deleted = await storage.cleanup_old_results(0)
    assert deleted == 0


@pytest.mark.asyncio
async def test_cleanup_removes_old_results():
    """Backdate a result then verify cleanup_old_results removes it."""
    async with _client() as c:
        r = await c.post("/process", files={"file": ("a.txt", _INVOICE, "text/plain")})
    doc_id = r.json()["document_id"]

    # Manually backdate the result to 10 days ago
    async with aiosqlite.connect(storage._db_path()) as db:
        await db.execute(
            "UPDATE results SET created_at = strftime('%Y-%m-%dT%H:%M:%SZ', datetime('now', '-10 days')) "
            "WHERE id = ?",
            (doc_id,),
        )
        await db.commit()

    deleted = await storage.cleanup_old_results(7)
    assert deleted == 1

    # Result and document should be gone
    assert await storage.get_result(doc_id) is None  # type: ignore[arg-type]
    assert await storage.get_document(doc_id) is None  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_cleanup_keeps_recent_results():
    async with _client() as c:
        await c.post("/process", files={"file": ("a.txt", _INVOICE, "text/plain")})
    deleted = await storage.cleanup_old_results(30)
    assert deleted == 0


@pytest.mark.asyncio
async def test_cleanup_also_removes_tags_and_notes():
    """cleanup_old_results cascades to tags and notes."""
    async with _client() as c:
        r = await c.post("/process", files={"file": ("a.txt", _INVOICE, "text/plain")})
    doc_id = r.json()["document_id"]

    await storage.add_tags(doc_id, ["important"])
    await storage.set_note(doc_id, "remember this")

    async with aiosqlite.connect(storage._db_path()) as db:
        await db.execute(
            "UPDATE results SET created_at = strftime('%Y-%m-%dT%H:%M:%SZ', datetime('now', '-10 days')) "
            "WHERE id = ?",
            (doc_id,),
        )
        await db.commit()

    await storage.cleanup_old_results(7)

    # Tags and notes should be gone
    tags = await storage.get_tags(doc_id)
    note = await storage.get_note(doc_id)
    assert tags == []
    assert note is None


@pytest.mark.asyncio
async def test_admin_cleanup_endpoint():
    async with _client() as c:
        r = await c.post("/process", files={"file": ("a.txt", _INVOICE, "text/plain")})
    doc_id = r.json()["document_id"]

    async with aiosqlite.connect(storage._db_path()) as db:
        await db.execute(
            "UPDATE results SET created_at = strftime('%Y-%m-%dT%H:%M:%SZ', datetime('now', '-10 days')) "
            "WHERE id = ?",
            (doc_id,),
        )
        await db.commit()

    async with _client() as c:
        r = await c.post("/admin/cleanup?older_than_days=7")
    assert r.status_code == 200
    assert r.json()["deleted"] == 1


@pytest.mark.asyncio
async def test_admin_retention_config_endpoint():
    async with _client() as c:
        r = await c.get("/admin/retention")
    assert r.status_code == 200
    body = r.json()
    assert "retention_days" in body
    assert "active" in body
    assert body["retention_days"] == 0
    assert body["active"] is False


@pytest.mark.asyncio
async def test_admin_cleanup_requires_older_than_days():
    async with _client() as c:
        r = await c.post("/admin/cleanup")
    assert r.status_code == 422  # missing required query param


# ---------------------------------------------------------------------------
# Document notes
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_set_and_get_note():
    async with _client() as c:
        upload = await c.post("/process", files={"file": ("a.txt", _INVOICE, "text/plain")})
        doc_id = upload.json()["document_id"]
        await c.put(f"/results/{doc_id}/note", json={"note": "Important invoice from January"})
        r = await c.get(f"/results/{doc_id}/note")
    assert r.status_code == 200
    assert r.json()["note"] == "Important invoice from January"
    assert "updated_at" in r.json()


@pytest.mark.asyncio
async def test_set_note_is_upsert():
    """Setting a note twice replaces the previous value."""
    async with _client() as c:
        upload = await c.post("/process", files={"file": ("a.txt", _INVOICE, "text/plain")})
        doc_id = upload.json()["document_id"]
        await c.put(f"/results/{doc_id}/note", json={"note": "first"})
        await c.put(f"/results/{doc_id}/note", json={"note": "second"})
        r = await c.get(f"/results/{doc_id}/note")
    assert r.json()["note"] == "second"


@pytest.mark.asyncio
async def test_get_note_404_when_not_set():
    async with _client() as c:
        upload = await c.post("/process", files={"file": ("a.txt", _INVOICE, "text/plain")})
        doc_id = upload.json()["document_id"]
        r = await c.get(f"/results/{doc_id}/note")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_delete_note():
    async with _client() as c:
        upload = await c.post("/process", files={"file": ("a.txt", _INVOICE, "text/plain")})
        doc_id = upload.json()["document_id"]
        await c.put(f"/results/{doc_id}/note", json={"note": "to be deleted"})
        del_r = await c.delete(f"/results/{doc_id}/note")
        get_r = await c.get(f"/results/{doc_id}/note")
    assert del_r.status_code == 204
    assert get_r.status_code == 404


@pytest.mark.asyncio
async def test_delete_missing_note_404():
    async with _client() as c:
        upload = await c.post("/process", files={"file": ("a.txt", _INVOICE, "text/plain")})
        doc_id = upload.json()["document_id"]
        r = await c.delete(f"/results/{doc_id}/note")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_note_endpoints_404_for_missing_result():
    async with _client() as c:
        fake_id = uuid4()
        put_r = await c.put(f"/results/{fake_id}/note", json={"note": "x"})
        get_r = await c.get(f"/results/{fake_id}/note")
        del_r = await c.delete(f"/results/{fake_id}/note")
    assert put_r.status_code == 404
    assert get_r.status_code == 404
    assert del_r.status_code == 404
