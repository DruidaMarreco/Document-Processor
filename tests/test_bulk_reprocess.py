"""Tests for POST /results/bulk-reprocess — concurrent multi-document reprocessing."""
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
# API endpoint tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_bulk_reprocess_200():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.post("/results/bulk-reprocess", json={"ids": [doc_id]})
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_bulk_reprocess_response_structure():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.post("/results/bulk-reprocess", json={"ids": [doc_id]})
    body = r.json()
    for field in ("requested", "succeeded", "skipped", "errored", "results"):
        assert field in body


@pytest.mark.asyncio
async def test_bulk_reprocess_counts_match():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL, "email.txt")
        r = await c.post("/results/bulk-reprocess", json={"ids": [id1, id2]})
    body = r.json()
    assert body["requested"] == 2
    assert body["succeeded"] + body["skipped"] + body["errored"] == 2


@pytest.mark.asyncio
async def test_bulk_reprocess_single_succeeds():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.post("/results/bulk-reprocess", json={"ids": [doc_id]})
    body = r.json()
    assert body["succeeded"] == 1
    assert body["results"][0]["status"] == "success"
    assert body["results"][0]["id"] == doc_id


@pytest.mark.asyncio
async def test_bulk_reprocess_result_has_pipeline_status():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.post("/results/bulk-reprocess", json={"ids": [doc_id]})
    result = r.json()["results"][0]
    assert "pipeline_status" in result


@pytest.mark.asyncio
async def test_bulk_reprocess_multiple_documents():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL, "email.txt")
        r = await c.post("/results/bulk-reprocess", json={"ids": [id1, id2]})
    body = r.json()
    assert body["succeeded"] == 2
    assert len(body["results"]) == 2


@pytest.mark.asyncio
async def test_bulk_reprocess_nonexistent_id_skipped():
    async with _client() as c:
        r = await c.post("/results/bulk-reprocess", json={"ids": [str(uuid4())]})
    body = r.json()
    assert body["skipped"] == 1
    assert body["results"][0]["status"] == "skipped"
    assert body["results"][0]["reason"] == "no_document"


@pytest.mark.asyncio
async def test_bulk_reprocess_locked_document_skipped():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.post(f"/results/{doc_id}/lock")
        r = await c.post("/results/bulk-reprocess", json={"ids": [doc_id]})
    body = r.json()
    assert body["skipped"] == 1
    assert body["results"][0]["reason"] == "locked"


@pytest.mark.asyncio
async def test_bulk_reprocess_mixed_valid_and_missing():
    async with _client() as c:
        doc_id = await _upload(c)
        missing_id = str(uuid4())
        r = await c.post("/results/bulk-reprocess", json={"ids": [doc_id, missing_id]})
    body = r.json()
    assert body["succeeded"] == 1
    assert body["skipped"] == 1


@pytest.mark.asyncio
async def test_bulk_reprocess_400_empty_ids():
    async with _client() as c:
        r = await c.post("/results/bulk-reprocess", json={"ids": []})
    assert r.status_code == 400


@pytest.mark.asyncio
async def test_bulk_reprocess_400_too_many_ids():
    async with _client() as c:
        r = await c.post("/results/bulk-reprocess", json={"ids": [str(uuid4()) for _ in range(21)]})
    assert r.status_code == 400


@pytest.mark.asyncio
async def test_bulk_reprocess_result_ids_match_input():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL, "email.txt")
        r = await c.post("/results/bulk-reprocess", json={"ids": [id1, id2]})
    result_ids = {r["id"] for r in r.json()["results"]}
    assert result_ids == {id1, id2}


@pytest.mark.asyncio
async def test_bulk_reprocess_updates_result():
    """After reprocess, the result should still be retrievable."""
    async with _client() as c:
        doc_id = await _upload(c)
        await c.post("/results/bulk-reprocess", json={"ids": [doc_id]})
        r = await c.get(f"/results/{doc_id}")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_bulk_reprocess_recorded_in_audit():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.post("/results/bulk-reprocess", json={"ids": [doc_id]})
        r = await c.get(f"/results/{doc_id}/audit")
    actions = [e["action"] for e in r.json()["entries"]]
    assert "reprocessed" in actions


@pytest.mark.asyncio
async def test_bulk_reprocess_20_ids_accepted():
    """Exactly 20 ids should be accepted (boundary)."""
    async with _client() as c:
        ids = [str(uuid4()) for _ in range(20)]
        r = await c.post("/results/bulk-reprocess", json={"ids": ids})
    assert r.status_code == 200
    assert r.json()["requested"] == 20
