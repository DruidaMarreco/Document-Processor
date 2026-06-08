"""Tests for content-hash deduplication and result tagging."""
from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient

from document_processor import storage
from document_processor.api import app

_INVOICE = b"Invoice No: 1234\nBill To: John\nAmount Due: $500\nDue Date: 2026-01-01"
_CONTRACT = b"This Agreement is entered into by the parties. Whereas hereinafter."


@pytest.fixture(autouse=True)
def tmp_db(tmp_path: Path):
    storage.set_db_path(tmp_path / "test.db")
    yield
    storage.set_db_path(Path("data/results.db"))


def _client():
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dedup_off_by_default_processes_both():
    """Without ?dedup=true, identical content creates two separate results."""
    async with _client() as c:
        r1 = await c.post("/process", files={"file": ("a.txt", _INVOICE, "text/plain")})
        r2 = await c.post("/process", files={"file": ("b.txt", _INVOICE, "text/plain")})
    assert r1.json()["document_id"] != r2.json()["document_id"]


@pytest.mark.asyncio
async def test_dedup_returns_same_result_for_identical_content():
    async with _client() as c:
        r1 = await c.post("/process?dedup=true",
                           files={"file": ("a.txt", _INVOICE, "text/plain")})
        r2 = await c.post("/process?dedup=true",
                           files={"file": ("b.txt", _INVOICE, "text/plain")})
    assert r1.json()["document_id"] == r2.json()["document_id"]


@pytest.mark.asyncio
async def test_dedup_header_set_on_cache_hit():
    async with _client() as c:
        await c.post("/process?dedup=true",
                     files={"file": ("a.txt", _INVOICE, "text/plain")})
        r2 = await c.post("/process?dedup=true",
                          files={"file": ("b.txt", _INVOICE, "text/plain")})
    assert r2.headers.get("x-dedup-result") == "true"
    assert "x-document-id" in r2.headers


@pytest.mark.asyncio
async def test_dedup_miss_on_different_content():
    """Different content → fresh processing, no cache hit."""
    async with _client() as c:
        r1 = await c.post("/process?dedup=true",
                           files={"file": ("inv.txt", _INVOICE, "text/plain")})
        r2 = await c.post("/process?dedup=true",
                           files={"file": ("con.txt", _CONTRACT, "text/plain")})
    assert r1.json()["document_id"] != r2.json()["document_id"]
    assert r2.headers.get("x-dedup-result") is None


@pytest.mark.asyncio
async def test_dedup_first_upload_no_cache_header():
    """First upload of a document never sets the cache header."""
    async with _client() as c:
        r = await c.post("/process?dedup=true",
                         files={"file": ("a.txt", _INVOICE, "text/plain")})
    assert r.headers.get("x-dedup-result") is None


@pytest.mark.asyncio
async def test_find_duplicate_storage_function():
    """Storage layer returns existing result for same bytes."""
    async with _client() as c:
        r = await c.post("/process",
                         files={"file": ("a.txt", _INVOICE, "text/plain")})
    original_id = r.json()["document_id"]
    dupe = await storage.find_duplicate(_INVOICE)
    assert dupe is not None
    assert str(dupe.document_id) == original_id


@pytest.mark.asyncio
async def test_find_duplicate_none_for_unseen_content():
    await storage.init_db()
    result = await storage.find_duplicate(b"totally new content xyz 12345")
    assert result is None


# ---------------------------------------------------------------------------
# Tagging
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_add_and_get_tags():
    async with _client() as c:
        upload = await c.post("/process", files={"file": ("a.txt", _INVOICE, "text/plain")})
        doc_id = upload.json()["document_id"]
        await c.post(f"/results/{doc_id}/tags", json={"tags": ["invoice", "2026", "client-a"]})
        r = await c.get(f"/results/{doc_id}/tags")
    assert r.status_code == 200
    assert set(r.json()["tags"]) == {"invoice", "2026", "client-a"}


@pytest.mark.asyncio
async def test_tags_normalised_to_lowercase():
    async with _client() as c:
        upload = await c.post("/process", files={"file": ("a.txt", _INVOICE, "text/plain")})
        doc_id = upload.json()["document_id"]
        await c.post(f"/results/{doc_id}/tags", json={"tags": ["Invoice", "URGENT"]})
        r = await c.get(f"/results/{doc_id}/tags")
    tags = r.json()["tags"]
    assert "invoice" in tags
    assert "urgent" in tags
    assert "Invoice" not in tags


@pytest.mark.asyncio
async def test_add_tags_idempotent():
    """Adding the same tag twice doesn't duplicate it."""
    async with _client() as c:
        upload = await c.post("/process", files={"file": ("a.txt", _INVOICE, "text/plain")})
        doc_id = upload.json()["document_id"]
        await c.post(f"/results/{doc_id}/tags", json={"tags": ["invoice"]})
        await c.post(f"/results/{doc_id}/tags", json={"tags": ["invoice"]})
        r = await c.get(f"/results/{doc_id}/tags")
    assert r.json()["tags"].count("invoice") == 1


@pytest.mark.asyncio
async def test_remove_tag():
    async with _client() as c:
        upload = await c.post("/process", files={"file": ("a.txt", _INVOICE, "text/plain")})
        doc_id = upload.json()["document_id"]
        await c.post(f"/results/{doc_id}/tags", json={"tags": ["keep", "remove"]})
        del_r = await c.delete(f"/results/{doc_id}/tags/remove")
        tags_r = await c.get(f"/results/{doc_id}/tags")
    assert del_r.status_code == 204
    assert "remove" not in tags_r.json()["tags"]
    assert "keep" in tags_r.json()["tags"]


@pytest.mark.asyncio
async def test_remove_missing_tag_404():
    async with _client() as c:
        upload = await c.post("/process", files={"file": ("a.txt", _INVOICE, "text/plain")})
        doc_id = upload.json()["document_id"]
        r = await c.delete(f"/results/{doc_id}/tags/nonexistent")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_tags_on_missing_result_404():
    async with _client() as c:
        r = await c.post(f"/results/{uuid4()}/tags", json={"tags": ["x"]})
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_filter_results_by_tag():
    async with _client() as c:
        u1 = await c.post("/process", files={"file": ("a.txt", _INVOICE, "text/plain")})
        u2 = await c.post("/process", files={"file": ("b.txt", _CONTRACT, "text/plain")})
        id1 = u1.json()["document_id"]
        await c.post(f"/results/{id1}/tags", json={"tags": ["urgent"]})
        r = await c.get("/results?tag=urgent")
    body = r.json()
    assert body["total"] == 1
    assert body["results"][0]["document_id"] == id1


@pytest.mark.asyncio
async def test_filter_by_tag_returns_empty_for_unmatched():
    async with _client() as c:
        await c.post("/process", files={"file": ("a.txt", _INVOICE, "text/plain")})
        r = await c.get("/results?tag=nonexistent-tag-xyz")
    assert r.json()["total"] == 0
    assert r.json()["results"] == []
