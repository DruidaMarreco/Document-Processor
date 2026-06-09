"""Tests for POST /results/bulk-delete and POST /results/bulk-tag."""
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
    r = await client.post("/process", files={"file": ("a.txt", _INVOICE, "text/plain")})
    assert r.status_code == 200
    return r.json()["document_id"]


# ---------------------------------------------------------------------------
# bulk-delete
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_bulk_delete_all():
    async with _client() as c:
        id1 = await _upload(c)
        id2 = await _upload(c)
        r = await c.post("/results/bulk-delete", json={"ids": [id1, id2]})
    assert r.status_code == 200
    assert r.json()["deleted"] == 2
    assert r.json()["requested"] == 2


@pytest.mark.asyncio
async def test_bulk_delete_removes_results():
    async with _client() as c:
        id1 = await _upload(c)
        id2 = await _upload(c)
        await c.post("/results/bulk-delete", json={"ids": [id1, id2]})
        r1 = await c.get(f"/results/{id1}")
        r2 = await c.get(f"/results/{id2}")
    assert r1.status_code == 404
    assert r2.status_code == 404


@pytest.mark.asyncio
async def test_bulk_delete_partial_missing():
    """Only existing IDs count toward deleted; missing IDs are silently skipped."""
    async with _client() as c:
        doc_id = await _upload(c)
        fake_id = str(uuid4())
        r = await c.post("/results/bulk-delete", json={"ids": [doc_id, fake_id]})
    assert r.status_code == 200
    body = r.json()
    assert body["deleted"] == 1
    assert body["requested"] == 2


@pytest.mark.asyncio
async def test_bulk_delete_all_missing():
    async with _client() as c:
        r = await c.post(
            "/results/bulk-delete",
            json={"ids": [str(uuid4()), str(uuid4())]},
        )
    assert r.status_code == 200
    assert r.json()["deleted"] == 0


@pytest.mark.asyncio
async def test_bulk_delete_empty_ids_rejected():
    async with _client() as c:
        r = await c.post("/results/bulk-delete", json={"ids": []})
    assert r.status_code == 400


@pytest.mark.asyncio
async def test_bulk_delete_over_limit_rejected():
    ids = [str(uuid4()) for _ in range(101)]
    async with _client() as c:
        r = await c.post("/results/bulk-delete", json={"ids": ids})
    assert r.status_code == 400


@pytest.mark.asyncio
async def test_bulk_delete_cascades_tags_and_notes():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.post(f"/results/{doc_id}/tags", json={"tags": ["keep"]})
        await c.put(f"/results/{doc_id}/note", json={"note": "remember"})
        await c.post("/results/bulk-delete", json={"ids": [doc_id]})
    tags = await storage.get_tags(doc_id)
    note = await storage.get_note(doc_id)
    assert tags == []
    assert note is None


# ---------------------------------------------------------------------------
# bulk-tag
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_bulk_tag_adds_to_all():
    async with _client() as c:
        id1 = await _upload(c)
        id2 = await _upload(c)
        r = await c.post("/results/bulk-tag", json={"ids": [id1, id2], "tags": ["urgent"]})
    assert r.status_code == 200
    assert r.json()["updated"] == 2
    assert "urgent" in await storage.get_tags(id1)
    assert "urgent" in await storage.get_tags(id2)


@pytest.mark.asyncio
async def test_bulk_tag_multiple_tags():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.post(
            "/results/bulk-tag",
            json={"ids": [doc_id], "tags": ["invoice", "q1", "reviewed"]},
        )
    assert r.status_code == 200
    tags = await storage.get_tags(doc_id)
    assert set(tags) == {"invoice", "q1", "reviewed"}


@pytest.mark.asyncio
async def test_bulk_tag_skips_missing_ids():
    async with _client() as c:
        doc_id = await _upload(c)
        fake = str(uuid4())
        r = await c.post(
            "/results/bulk-tag",
            json={"ids": [doc_id, fake], "tags": ["test"]},
        )
    assert r.status_code == 200
    assert r.json()["updated"] == 1


@pytest.mark.asyncio
async def test_bulk_tag_idempotent():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.post("/results/bulk-tag", json={"ids": [doc_id], "tags": ["dup"]})
        r = await c.post("/results/bulk-tag", json={"ids": [doc_id], "tags": ["dup"]})
    assert r.status_code == 200
    tags = await storage.get_tags(doc_id)
    assert tags.count("dup") == 1


@pytest.mark.asyncio
async def test_bulk_tag_empty_ids_rejected():
    async with _client() as c:
        r = await c.post("/results/bulk-tag", json={"ids": [], "tags": ["x"]})
    assert r.status_code == 400


@pytest.mark.asyncio
async def test_bulk_tag_empty_tags_rejected():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.post("/results/bulk-tag", json={"ids": [doc_id], "tags": []})
    assert r.status_code == 400


@pytest.mark.asyncio
async def test_bulk_tag_over_limit_rejected():
    ids = [str(uuid4()) for _ in range(101)]
    async with _client() as c:
        r = await c.post("/results/bulk-tag", json={"ids": ids, "tags": ["x"]})
    assert r.status_code == 400
