"""Tests for result access log — GET /results/{id}/access-log and /access-count."""
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
async def test_access_count_zero_initially():
    async with _client() as c:
        doc_id = await _upload(c)
    count = await storage.get_result_access_count(__import__("uuid").UUID(doc_id))
    assert count == 0


@pytest.mark.asyncio
async def test_record_access_increments_count():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    await storage.record_result_access(uid)
    assert await storage.get_result_access_count(uid) == 1


@pytest.mark.asyncio
async def test_multiple_access_records():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    for _ in range(5):
        await storage.record_result_access(uid)
    assert await storage.get_result_access_count(uid) == 5


@pytest.mark.asyncio
async def test_access_log_entries_have_timestamps():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    await storage.record_result_access(uid)
    entries, total = await storage.get_result_access_log(uid)
    assert total == 1
    assert "accessed_at" in entries[0]


@pytest.mark.asyncio
async def test_access_log_entries_have_ids():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    await storage.record_result_access(uid)
    entries, _ = await storage.get_result_access_log(uid)
    assert "id" in entries[0]


@pytest.mark.asyncio
async def test_access_log_pagination():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    for _ in range(10):
        await storage.record_result_access(uid)
    entries, total = await storage.get_result_access_log(uid, limit=3, offset=0)
    assert total == 10
    assert len(entries) == 3


@pytest.mark.asyncio
async def test_access_log_independent_per_result():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL, "email.txt")
    uid1 = __import__("uuid").UUID(id1)
    uid2 = __import__("uuid").UUID(id2)
    await storage.record_result_access(uid1)
    await storage.record_result_access(uid1)
    assert await storage.get_result_access_count(uid1) == 2
    assert await storage.get_result_access_count(uid2) == 0


# ---------------------------------------------------------------------------
# API endpoint tests — GET /results/{id} records access
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_result_records_access():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.get(f"/results/{doc_id}")
        r = await c.get(f"/results/{doc_id}/access-count")
    assert r.json()["access_count"] == 1


@pytest.mark.asyncio
async def test_get_result_multiple_times_increments():
    async with _client() as c:
        doc_id = await _upload(c)
        for _ in range(3):
            await c.get(f"/results/{doc_id}")
        r = await c.get(f"/results/{doc_id}/access-count")
    assert r.json()["access_count"] == 3


@pytest.mark.asyncio
async def test_access_log_endpoint_200():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.get(f"/results/{doc_id}")
        r = await c.get(f"/results/{doc_id}/access-log")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_access_log_response_structure():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.get(f"/results/{doc_id}")
        r = await c.get(f"/results/{doc_id}/access-log")
    body = r.json()
    assert "document_id" in body
    assert "total" in body
    assert "entries" in body


@pytest.mark.asyncio
async def test_access_log_entries_populated():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.get(f"/results/{doc_id}")
        await c.get(f"/results/{doc_id}")
        r = await c.get(f"/results/{doc_id}/access-log")
    body = r.json()
    assert body["total"] == 2
    assert len(body["entries"]) == 2


@pytest.mark.asyncio
async def test_access_log_empty_initially():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.get(f"/results/{doc_id}/access-log")
    body = r.json()
    assert body["total"] == 0
    assert body["entries"] == []


@pytest.mark.asyncio
async def test_access_count_endpoint_200():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.get(f"/results/{doc_id}/access-count")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_access_count_zero_initially_via_api():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.get(f"/results/{doc_id}/access-count")
    assert r.json()["access_count"] == 0


@pytest.mark.asyncio
async def test_access_log_404_missing():
    async with _client() as c:
        r = await c.get(f"/results/{uuid4()}/access-log")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_access_count_404_missing():
    async with _client() as c:
        r = await c.get(f"/results/{uuid4()}/access-count")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_access_log_document_id_in_response():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.get(f"/results/{doc_id}/access-log")
    assert r.json()["document_id"] == doc_id


@pytest.mark.asyncio
async def test_access_count_document_id_in_response():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.get(f"/results/{doc_id}/access-count")
    assert r.json()["document_id"] == doc_id
