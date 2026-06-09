"""Tests for result reactions — POST/DELETE/GET /results/{id}/reactions."""
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
    r = await client.post("/process", files={"file": ("doc.txt", _INVOICE, "text/plain")})
    assert r.status_code == 200
    return r.json()["document_id"]


# ---------------------------------------------------------------------------
# Storage unit tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_reactions_empty_initially():
    async with _client() as c:
        doc_id = await _upload(c)
    counts = await storage.get_reactions(__import__("uuid").UUID(doc_id))
    assert counts == {}


@pytest.mark.asyncio
async def test_add_reaction_increments_count():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    counts = await storage.add_reaction(uid, "+1")
    assert counts["+1"] == 1


@pytest.mark.asyncio
async def test_add_reaction_twice():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    await storage.add_reaction(uid, "+1")
    counts = await storage.add_reaction(uid, "+1")
    assert counts["+1"] == 2


@pytest.mark.asyncio
async def test_multiple_different_reactions():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    await storage.add_reaction(uid, "+1")
    await storage.add_reaction(uid, "eyes")
    counts = await storage.get_reactions(uid)
    assert counts["+1"] == 1
    assert counts["eyes"] == 1


@pytest.mark.asyncio
async def test_remove_reaction_decrements():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    await storage.add_reaction(uid, "+1")
    await storage.add_reaction(uid, "+1")
    counts = await storage.remove_reaction(uid, "+1")
    assert counts["+1"] == 1


@pytest.mark.asyncio
async def test_remove_reaction_to_zero_clears_key():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    await storage.add_reaction(uid, "+1")
    counts = await storage.remove_reaction(uid, "+1")
    assert "+1" not in counts


@pytest.mark.asyncio
async def test_remove_reaction_floor_zero():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    # Remove a reaction that was never added — should not error, count stays at 0/absent
    counts = await storage.remove_reaction(uid, "+1")
    assert "+1" not in counts


# ---------------------------------------------------------------------------
# API endpoint tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_add_reaction_200():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.post(f"/results/{doc_id}/reactions", json={"emoji": "+1"})
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_add_reaction_response_structure():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.post(f"/results/{doc_id}/reactions", json={"emoji": "+1"})
    body = r.json()
    assert "document_id" in body
    assert "reactions" in body
    assert body["reactions"]["+1"] == 1


@pytest.mark.asyncio
async def test_add_reaction_invalid_emoji():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.post(f"/results/{doc_id}/reactions", json={"emoji": "rocket"})
    assert r.status_code == 400


@pytest.mark.asyncio
async def test_add_reaction_404_missing():
    async with _client() as c:
        r = await c.post(f"/results/{uuid4()}/reactions", json={"emoji": "+1"})
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_remove_reaction_200():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.post(f"/results/{doc_id}/reactions", json={"emoji": "+1"})
        r = await c.delete(f"/results/{doc_id}/reactions/+1")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_remove_reaction_invalid_emoji():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.delete(f"/results/{doc_id}/reactions/rocket")
    assert r.status_code == 400


@pytest.mark.asyncio
async def test_remove_reaction_404_missing_result():
    async with _client() as c:
        r = await c.delete(f"/results/{uuid4()}/reactions/+1")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_get_reactions_200():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.get(f"/results/{doc_id}/reactions")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_get_reactions_empty():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.get(f"/results/{doc_id}/reactions")
    assert r.json()["reactions"] == {}


@pytest.mark.asyncio
async def test_get_reactions_after_add():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.post(f"/results/{doc_id}/reactions", json={"emoji": "check"})
        r = await c.get(f"/results/{doc_id}/reactions")
    assert r.json()["reactions"]["check"] == 1


@pytest.mark.asyncio
async def test_get_reactions_404_missing():
    async with _client() as c:
        r = await c.get(f"/results/{uuid4()}/reactions")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_all_allowed_reactions_accepted():
    async with _client() as c:
        doc_id = await _upload(c)
        for emoji in storage.ALLOWED_REACTIONS:
            r = await c.post(f"/results/{doc_id}/reactions", json={"emoji": emoji})
            assert r.status_code == 200


@pytest.mark.asyncio
async def test_reaction_recorded_in_audit():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.post(f"/results/{doc_id}/reactions", json={"emoji": "+1"})
        r = await c.get(f"/results/{doc_id}/audit")
    actions = [e["action"] for e in r.json()["entries"]]
    assert "reaction_added" in actions


@pytest.mark.asyncio
async def test_reactions_independent_per_result():
    async with _client() as c:
        id1 = await _upload(c)
        id2 = await _upload(c)
        await c.post(f"/results/{id1}/reactions", json={"emoji": "+1"})
        r1 = await c.get(f"/results/{id1}/reactions")
        r2 = await c.get(f"/results/{id2}/reactions")
    assert r1.json()["reactions"].get("+1", 0) == 1
    assert r2.json()["reactions"].get("+1", 0) == 0
