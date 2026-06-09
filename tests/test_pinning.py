"""Tests for result pinning (PUT/DELETE/GET /results/{id}/pin)."""
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


async def _upload(client) -> str:
    r = await client.post("/process", files={"file": ("a.txt", _INVOICE, "text/plain")})
    assert r.status_code == 200
    return r.json()["document_id"]


# ---------------------------------------------------------------------------
# Basic pin / unpin / get state
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_pin_result():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.put(f"/results/{doc_id}/pin")
    assert r.status_code == 204


@pytest.mark.asyncio
async def test_get_pin_state_true():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.put(f"/results/{doc_id}/pin")
        r = await c.get(f"/results/{doc_id}/pin")
    assert r.status_code == 200
    assert r.json()["pinned"] is True


@pytest.mark.asyncio
async def test_get_pin_state_false_by_default():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.get(f"/results/{doc_id}/pin")
    assert r.status_code == 200
    assert r.json()["pinned"] is False


@pytest.mark.asyncio
async def test_unpin_result():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.put(f"/results/{doc_id}/pin")
        r_unpin = await c.delete(f"/results/{doc_id}/pin")
        r_state = await c.get(f"/results/{doc_id}/pin")
    assert r_unpin.status_code == 204
    assert r_state.json()["pinned"] is False


@pytest.mark.asyncio
async def test_unpin_not_pinned_returns_409():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.delete(f"/results/{doc_id}/pin")
    assert r.status_code == 409


@pytest.mark.asyncio
async def test_pin_missing_result_404():
    async with _client() as c:
        r = await c.put(f"/results/{uuid4()}/pin")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_get_pin_missing_result_404():
    async with _client() as c:
        r = await c.get(f"/results/{uuid4()}/pin")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_unpin_missing_result_404():
    async with _client() as c:
        r = await c.delete(f"/results/{uuid4()}/pin")
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Pin filter on /results
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_filter_pinned_true():
    async with _client() as c:
        id_pinned = await _upload(c)
        await _upload(c)  # unpinned
        await c.put(f"/results/{id_pinned}/pin")
        r = await c.get("/results?pinned=true")
    body = r.json()
    assert body["total"] == 1
    assert body["results"][0]["document_id"] == id_pinned


@pytest.mark.asyncio
async def test_filter_pinned_false():
    async with _client() as c:
        id_pinned = await _upload(c)
        id_unpinned = await _upload(c)
        await c.put(f"/results/{id_pinned}/pin")
        r = await c.get("/results?pinned=false")
    ids = [res["document_id"] for res in r.json()["results"]]
    assert id_unpinned in ids
    assert id_pinned not in ids


@pytest.mark.asyncio
async def test_no_pin_filter_returns_all():
    async with _client() as c:
        id1 = await _upload(c)
        id2 = await _upload(c)
        await c.put(f"/results/{id1}/pin")
        r = await c.get("/results")
    ids = [res["document_id"] for res in r.json()["results"]]
    assert id1 in ids
    assert id2 in ids


# ---------------------------------------------------------------------------
# Pinned results survive retention cleanup
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_pinned_result_survives_retention():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.put(f"/results/{doc_id}/pin")

    # Backdate to 10 days ago
    async with aiosqlite.connect(storage._db_path()) as db:
        await db.execute(
            "UPDATE results SET created_at = strftime('%Y-%m-%dT%H:%M:%SZ', datetime('now', '-10 days')) "
            "WHERE id = ?",
            (doc_id,),
        )
        await db.commit()

    deleted = await storage.cleanup_old_results(7)
    assert deleted == 0
    assert await storage.get_result(doc_id) is not None


@pytest.mark.asyncio
async def test_unpinned_result_removed_by_retention():
    async with _client() as c:
        doc_id = await _upload(c)

    async with aiosqlite.connect(storage._db_path()) as db:
        await db.execute(
            "UPDATE results SET created_at = strftime('%Y-%m-%dT%H:%M:%SZ', datetime('now', '-10 days')) "
            "WHERE id = ?",
            (doc_id,),
        )
        await db.commit()

    deleted = await storage.cleanup_old_results(7)
    assert deleted == 1
    assert await storage.get_result(doc_id) is None


# ---------------------------------------------------------------------------
# Audit entries for pin/unpin
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_pin_unpin_recorded_in_audit():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.put(f"/results/{doc_id}/pin")
        await c.delete(f"/results/{doc_id}/pin")
    entries = await storage.get_audit_log(doc_id)
    actions = [e["action"] for e in entries]
    assert "pinned" in actions
    assert "unpinned" in actions
