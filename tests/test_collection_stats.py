"""Tests for GET /collections/{id}/stats — aggregate statistics per collection."""
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


async def _create_collection(client, name: str = "test-col") -> str:
    r = await client.post("/collections", json={"name": name})
    assert r.status_code == 201
    return r.json()["id"]


async def _add_to_collection(client, col_id: str, doc_ids: list[str]) -> None:
    await client.post(f"/collections/{col_id}/members", json={"ids": doc_ids})


# ---------------------------------------------------------------------------
# Storage unit tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_collection_stats_returns_none_for_missing():
    result = await storage.get_collection_stats(str(uuid4()))
    assert result is None


@pytest.mark.asyncio
async def test_collection_stats_empty_collection():
    async with _client() as c:
        col_id = await _create_collection(c)
    stats = await storage.get_collection_stats(col_id)
    assert stats is not None
    assert stats["total"] == 0
    assert stats["by_doc_type"] == []
    assert stats["by_status"] == []
    assert stats["avg_duration_ms"] is None


@pytest.mark.asyncio
async def test_collection_stats_total_count():
    async with _client() as c:
        col_id = await _create_collection(c)
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL, "email.txt")
        await _add_to_collection(c, col_id, [id1, id2])
    stats = await storage.get_collection_stats(col_id)
    assert stats["total"] == 2


@pytest.mark.asyncio
async def test_collection_stats_has_required_fields():
    async with _client() as c:
        col_id = await _create_collection(c)
    stats = await storage.get_collection_stats(col_id)
    for field in ("collection_id", "total", "pinned", "locked",
                  "avg_duration_ms", "by_doc_type", "by_status"):
        assert field in stats


@pytest.mark.asyncio
async def test_collection_stats_collection_id_matches():
    async with _client() as c:
        col_id = await _create_collection(c)
    stats = await storage.get_collection_stats(col_id)
    assert stats["collection_id"] == col_id


@pytest.mark.asyncio
async def test_collection_stats_by_status_populated():
    async with _client() as c:
        col_id = await _create_collection(c)
        doc_id = await _upload(c)
        await _add_to_collection(c, col_id, [doc_id])
    stats = await storage.get_collection_stats(col_id)
    assert len(stats["by_status"]) >= 1
    assert all("status" in s and "count" in s for s in stats["by_status"])


@pytest.mark.asyncio
async def test_collection_stats_by_doc_type_populated():
    async with _client() as c:
        col_id = await _create_collection(c)
        doc_id = await _upload(c)
        await _add_to_collection(c, col_id, [doc_id])
    stats = await storage.get_collection_stats(col_id)
    assert len(stats["by_doc_type"]) >= 1
    assert all("doc_type" in s and "count" in s for s in stats["by_doc_type"])


@pytest.mark.asyncio
async def test_collection_stats_avg_duration_non_negative():
    async with _client() as c:
        col_id = await _create_collection(c)
        doc_id = await _upload(c)
        await _add_to_collection(c, col_id, [doc_id])
    stats = await storage.get_collection_stats(col_id)
    if stats["avg_duration_ms"] is not None:
        assert stats["avg_duration_ms"] >= 0


@pytest.mark.asyncio
async def test_collection_stats_pinned_count():
    async with _client() as c:
        col_id = await _create_collection(c)
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL, "email.txt")
        await _add_to_collection(c, col_id, [id1, id2])
        await c.put(f"/results/{id1}/pin")
    stats = await storage.get_collection_stats(col_id)
    assert stats["pinned"] == 1


@pytest.mark.asyncio
async def test_collection_stats_locked_count():
    async with _client() as c:
        col_id = await _create_collection(c)
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL, "email.txt")
        await _add_to_collection(c, col_id, [id1, id2])
        await c.post(f"/results/{id1}/lock")
        await c.post(f"/results/{id2}/lock")
    stats = await storage.get_collection_stats(col_id)
    assert stats["locked"] == 2


# ---------------------------------------------------------------------------
# API endpoint tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_stats_endpoint_200():
    async with _client() as c:
        col_id = await _create_collection(c)
        r = await c.get(f"/collections/{col_id}/stats")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_stats_endpoint_404_for_missing():
    async with _client() as c:
        r = await c.get(f"/collections/{uuid4()}/stats")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_stats_response_structure():
    async with _client() as c:
        col_id = await _create_collection(c)
        r = await c.get(f"/collections/{col_id}/stats")
    body = r.json()
    for field in ("collection_id", "total", "pinned", "locked",
                  "avg_duration_ms", "by_doc_type", "by_status"):
        assert field in body


@pytest.mark.asyncio
async def test_stats_empty_collection():
    async with _client() as c:
        col_id = await _create_collection(c)
        r = await c.get(f"/collections/{col_id}/stats")
    body = r.json()
    assert body["total"] == 0
    assert body["by_doc_type"] == []
    assert body["by_status"] == []


@pytest.mark.asyncio
async def test_stats_total_reflects_members():
    async with _client() as c:
        col_id = await _create_collection(c)
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL, "email.txt")
        await _add_to_collection(c, col_id, [id1, id2])
        r = await c.get(f"/collections/{col_id}/stats")
    assert r.json()["total"] == 2


@pytest.mark.asyncio
async def test_stats_by_status_counts_add_to_total():
    async with _client() as c:
        col_id = await _create_collection(c)
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL, "email.txt")
        await _add_to_collection(c, col_id, [id1, id2])
        r = await c.get(f"/collections/{col_id}/stats")
    body = r.json()
    status_sum = sum(s["count"] for s in body["by_status"])
    assert status_sum == body["total"]


@pytest.mark.asyncio
async def test_stats_by_doc_type_counts_add_to_total():
    async with _client() as c:
        col_id = await _create_collection(c)
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL, "email.txt")
        await _add_to_collection(c, col_id, [id1, id2])
        r = await c.get(f"/collections/{col_id}/stats")
    body = r.json()
    type_sum = sum(s["count"] for s in body["by_doc_type"])
    assert type_sum == body["total"]


@pytest.mark.asyncio
async def test_stats_independent_per_collection():
    async with _client() as c:
        col1 = await _create_collection(c, "col-one")
        col2 = await _create_collection(c, "col-two")
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL, "email.txt")
        await _add_to_collection(c, col1, [id1])
        await _add_to_collection(c, col2, [id1, id2])
        r1 = await c.get(f"/collections/{col1}/stats")
        r2 = await c.get(f"/collections/{col2}/stats")
    assert r1.json()["total"] == 1
    assert r2.json()["total"] == 2
