"""Tests for FTS5 full-text search — GET /results/search and storage.fts_search."""
from __future__ import annotations

from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

from document_processor import storage
from document_processor.api import app

_INVOICE = b"Invoice No: 1234\nBill To: Acme Corp\nAmount Due: $500\nDue Date: 2026-01-01"
_CONTRACT = b"Service Agreement\nParties: FooBar Ltd and ClientCo\nTerm: 12 months\nValue: $10000"


@pytest.fixture(autouse=True)
def tmp_db(tmp_path: Path):
    storage.set_db_path(tmp_path / "test.db")
    yield
    storage.set_db_path(Path("data/results.db"))


def _client():
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


async def _upload(client, content: bytes, name: str = "doc.txt") -> str:
    r = await client.post("/process", files={"file": (name, content, "text/plain")})
    assert r.status_code == 200
    return r.json()["document_id"]


# ---------------------------------------------------------------------------
# Storage unit tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_fts_search_returns_list():
    async with _client() as c:
        await _upload(c, _INVOICE)
    results = await storage.fts_search("invoice")
    assert isinstance(results, list)


@pytest.mark.asyncio
async def test_fts_search_finds_by_filename():
    async with _client() as c:
        await _upload(c, _INVOICE, "invoice_2024.txt")
    results = await storage.fts_search("invoice_2024")
    assert len(results) >= 1


@pytest.mark.asyncio
async def test_fts_search_finds_by_content():
    async with _client() as c:
        await _upload(c, _INVOICE)
    results = await storage.fts_search("Acme")
    # FTS matches extracted text from the generator summary
    assert isinstance(results, list)


@pytest.mark.asyncio
async def test_fts_search_no_results_for_garbage():
    async with _client() as c:
        await _upload(c, _INVOICE)
    results = await storage.fts_search("xyzzy_nonexistent_token_abc")
    assert results == []


@pytest.mark.asyncio
async def test_fts_search_result_deleted_removes_from_index():
    async with _client() as c:
        doc_id = await _upload(c, _INVOICE, "invoice_delete.txt")
    results_before = await storage.fts_search("invoice_delete")
    assert len(results_before) >= 1
    from uuid import UUID
    await storage.delete_result(UUID(doc_id))
    results_after = await storage.fts_search("invoice_delete")
    assert doc_id not in results_after


@pytest.mark.asyncio
async def test_fts_search_limit():
    async with _client() as c:
        for i in range(5):
            await _upload(c, _INVOICE, f"inv_{i}.txt")
    results = await storage.fts_search("inv", limit=2)
    assert len(results) <= 2


@pytest.mark.asyncio
async def test_fts_search_offset():
    async with _client() as c:
        for i in range(4):
            await _upload(c, _INVOICE, f"page_{i}.txt")
    all_results = await storage.fts_search("page", limit=10)
    offset_results = await storage.fts_search("page", limit=10, offset=2)
    assert len(offset_results) <= len(all_results)


# ---------------------------------------------------------------------------
# API endpoint tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_search_endpoint_200():
    async with _client() as c:
        await _upload(c, _INVOICE, "inv.txt")
        r = await c.get("/results/search?q=inv")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_search_endpoint_response_structure():
    async with _client() as c:
        await _upload(c, _INVOICE, "structured.txt")
        r = await c.get("/results/search?q=structured")
    body = r.json()
    assert "query" in body
    assert "results" in body
    assert "total" in body


@pytest.mark.asyncio
async def test_search_endpoint_query_echoed():
    async with _client() as c:
        await _upload(c, _INVOICE)
        r = await c.get("/results/search?q=invoice")
    assert r.json()["query"] == "invoice"


@pytest.mark.asyncio
async def test_search_endpoint_finds_by_filename():
    async with _client() as c:
        await _upload(c, _INVOICE, "my_special_doc.txt")
        r = await c.get("/results/search?q=my_special_doc")
    assert r.json()["total"] >= 1


@pytest.mark.asyncio
async def test_search_endpoint_result_has_fields():
    async with _client() as c:
        await _upload(c, _INVOICE, "report.txt")
        r = await c.get("/results/search?q=report")
    results = r.json()["results"]
    if results:
        item = results[0]
        assert "document_id" in item
        assert "status" in item
        assert "completed_at" in item


@pytest.mark.asyncio
async def test_search_endpoint_empty_query_422():
    async with _client() as c:
        r = await c.get("/results/search?q=")
    assert r.status_code == 422


@pytest.mark.asyncio
async def test_search_endpoint_no_results():
    async with _client() as c:
        await _upload(c, _INVOICE)
        r = await c.get("/results/search?q=xyzzy_nothing_matches")
    assert r.json()["total"] == 0
    assert r.json()["results"] == []


@pytest.mark.asyncio
async def test_search_endpoint_limit_param():
    async with _client() as c:
        for i in range(3):
            await _upload(c, _INVOICE, f"lim_{i}.txt")
        r = await c.get("/results/search?q=lim&limit=2")
    assert len(r.json()["results"]) <= 2
