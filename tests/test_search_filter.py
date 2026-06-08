"""Tests for enhanced /results search: keyword, date range, and sorting."""
from __future__ import annotations

from pathlib import Path

import aiosqlite
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


async def _upload(client, content: bytes, filename: str = "doc.txt") -> str:
    r = await client.post("/process", files={"file": (filename, content, "text/plain")})
    assert r.status_code == 200
    return r.json()["document_id"]


# ---------------------------------------------------------------------------
# Keyword search
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_keyword_search_matches():
    async with _client() as c:
        doc_id = await _upload(c, _INVOICE, "invoice.txt")
        r = await c.get("/results?q=Invoice")
    assert r.status_code == 200
    body = r.json()
    assert body["total"] >= 1
    ids = [res["document_id"] for res in body["results"]]
    assert doc_id in ids


@pytest.mark.asyncio
async def test_keyword_search_no_match():
    async with _client() as c:
        await _upload(c, _INVOICE, "invoice.txt")
        r = await c.get("/results?q=xyzzy_impossible_string")
    assert r.status_code == 200
    assert r.json()["total"] == 0


@pytest.mark.asyncio
async def test_keyword_search_multiple_docs():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE, "invoice.txt")
        id2 = await _upload(c, _EMAIL, "email.txt")
        r_invoice = await c.get("/results?q=Invoice+No")
        r_email = await c.get("/results?q=alice%40example.com")
    inv_ids = [res["document_id"] for res in r_invoice.json()["results"]]
    email_ids = [res["document_id"] for res in r_email.json()["results"]]
    assert id1 in inv_ids
    assert id2 not in inv_ids
    assert id2 in email_ids
    assert id1 not in email_ids


# ---------------------------------------------------------------------------
# Date range filtering
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_date_from_filters_older():
    """date_from in the future should return nothing."""
    async with _client() as c:
        await _upload(c, _INVOICE, "invoice.txt")
        r = await c.get("/results?date_from=2030-01-01T00:00:00Z")
    assert r.json()["total"] == 0


@pytest.mark.asyncio
async def test_date_to_filters_newer():
    """date_to in the past should return nothing."""
    async with _client() as c:
        await _upload(c, _INVOICE, "invoice.txt")
        r = await c.get("/results?date_to=2000-01-01T00:00:00Z")
    assert r.json()["total"] == 0


@pytest.mark.asyncio
async def test_date_range_includes_recent():
    async with _client() as c:
        doc_id = await _upload(c, _INVOICE, "invoice.txt")
        r = await c.get("/results?date_from=2020-01-01T00:00:00Z&date_to=2030-12-31T23:59:59Z")
    assert r.json()["total"] >= 1
    ids = [res["document_id"] for res in r.json()["results"]]
    assert doc_id in ids


# ---------------------------------------------------------------------------
# Sorting
# ---------------------------------------------------------------------------

async def _backdate(doc_id: str, days_ago: int) -> None:
    """Backdate a result's created_at to make ordering deterministic in tests."""
    async with aiosqlite.connect(storage._db_path()) as db:
        await db.execute(
            "UPDATE results SET created_at = strftime('%Y-%m-%dT%H:%M:%SZ', "
            f"datetime('now', '-{days_ago} days')) WHERE id = ?",
            (doc_id,),
        )
        await db.commit()


@pytest.mark.asyncio
async def test_sort_by_created_at_desc_default():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE, "first.txt")
        id2 = await _upload(c, _EMAIL, "second.txt")
    # Make id1 clearly older
    await _backdate(id1, 1)
    async with _client() as c:
        r = await c.get("/results?sort_by=created_at&sort_order=desc")
    results = r.json()["results"]
    assert len(results) >= 2
    ids = [res["document_id"] for res in results]
    # Most recent first — id2 is newer
    assert ids.index(id2) < ids.index(id1)


@pytest.mark.asyncio
async def test_sort_by_created_at_asc():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE, "first.txt")
        id2 = await _upload(c, _EMAIL, "second.txt")
    await _backdate(id1, 1)
    async with _client() as c:
        r = await c.get("/results?sort_by=created_at&sort_order=asc")
    results = r.json()["results"]
    ids = [res["document_id"] for res in results]
    # Oldest first — id1 was backdated
    assert ids.index(id1) < ids.index(id2)


@pytest.mark.asyncio
async def test_sort_by_unknown_field_falls_back_to_created_at():
    """An unknown sort_by value should not error and should return results."""
    async with _client() as c:
        await _upload(c, _INVOICE, "invoice.txt")
        r = await c.get("/results?sort_by=nonexistent_column&sort_order=desc")
    assert r.status_code == 200
    assert r.json()["total"] >= 1


# ---------------------------------------------------------------------------
# Combined filters
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_combined_keyword_and_status():
    async with _client() as c:
        doc_id = await _upload(c, _INVOICE, "invoice.txt")
        r = await c.get("/results?q=Invoice&status=success")
    # May or may not find the doc depending on classification, but must not error
    assert r.status_code == 200
    assert "total" in r.json()


@pytest.mark.asyncio
async def test_combined_keyword_and_date_range():
    async with _client() as c:
        doc_id = await _upload(c, _INVOICE, "invoice.txt")
        r = await c.get(
            "/results?q=Invoice&date_from=2020-01-01T00:00:00Z&date_to=2030-12-31T23:59:59Z"
        )
    assert r.status_code == 200
    assert r.json()["total"] >= 1


@pytest.mark.asyncio
async def test_pagination_with_filters():
    async with _client() as c:
        for i in range(3):
            await _upload(c, _INVOICE + f"\nExtra: {i}".encode(), f"inv{i}.txt")
        r_page1 = await c.get("/results?q=Invoice&limit=2&offset=0")
        r_page2 = await c.get("/results?q=Invoice&limit=2&offset=2")
    total = r_page1.json()["total"]
    assert total >= 3
    ids_page1 = {res["document_id"] for res in r_page1.json()["results"]}
    ids_page2 = {res["document_id"] for res in r_page2.json()["results"]}
    assert len(ids_page1) == 2
    assert ids_page1.isdisjoint(ids_page2)
