"""Tests for the immutable audit log (GET /results/{id}/audit)."""
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
# Basic audit creation
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_process_creates_audit_entry():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.get(f"/results/{doc_id}/audit")
    assert r.status_code == 200
    body = r.json()
    assert body["document_id"] == doc_id
    assert len(body["entries"]) >= 1
    actions = [e["action"] for e in body["entries"]]
    assert "processed" in actions


@pytest.mark.asyncio
async def test_audit_entry_structure():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.get(f"/results/{doc_id}/audit")
    entry = r.json()["entries"][0]
    assert "id" in entry
    assert "action" in entry
    assert "created_at" in entry
    assert "detail" in entry


@pytest.mark.asyncio
async def test_tags_action_recorded():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.post(f"/results/{doc_id}/tags", json={"tags": ["important"]})
        r = await c.get(f"/results/{doc_id}/audit")
    actions = [e["action"] for e in r.json()["entries"]]
    assert "tags_added" in actions


@pytest.mark.asyncio
async def test_note_set_action_recorded():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.put(f"/results/{doc_id}/note", json={"note": "test note"})
        r = await c.get(f"/results/{doc_id}/audit")
    actions = [e["action"] for e in r.json()["entries"]]
    assert "note_set" in actions


@pytest.mark.asyncio
async def test_audit_entries_ordered_oldest_first():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.post(f"/results/{doc_id}/tags", json={"tags": ["a"]})
        await c.put(f"/results/{doc_id}/note", json={"note": "n"})
        r = await c.get(f"/results/{doc_id}/audit")
    entries = r.json()["entries"]
    assert len(entries) >= 3
    # IDs should be monotonically increasing (oldest first)
    ids = [e["id"] for e in entries]
    assert ids == sorted(ids)


@pytest.mark.asyncio
async def test_audit_actions_accumulate():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.post(f"/results/{doc_id}/tags", json={"tags": ["x"]})
        await c.post(f"/results/{doc_id}/tags", json={"tags": ["y"]})
        r = await c.get(f"/results/{doc_id}/audit")
    actions = [e["action"] for e in r.json()["entries"]]
    assert actions.count("tags_added") == 2


# ---------------------------------------------------------------------------
# 404 handling
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_audit_404_for_missing_result():
    async with _client() as c:
        r = await c.get(f"/results/{uuid4()}/audit")
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Limit parameter
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_audit_limit_param():
    async with _client() as c:
        doc_id = await _upload(c)
        for i in range(5):
            await c.post(f"/results/{doc_id}/tags", json={"tags": [f"tag{i}"]})
        r = await c.get(f"/results/{doc_id}/audit?limit=3")
    assert len(r.json()["entries"]) == 3


# ---------------------------------------------------------------------------
# Delete audit entry recorded before deletion
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_delete_action_recorded_before_deletion():
    """The 'deleted' audit entry is written before the result is removed."""
    async with _client() as c:
        doc_id = await _upload(c)
        await c.delete(f"/results/{doc_id}")
    # After deletion, result is gone — read audit directly from storage
    entries = await storage.get_audit_log(doc_id)
    actions = [e["action"] for e in entries]
    assert "deleted" in actions


# ---------------------------------------------------------------------------
# append_audit storage helper
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_append_audit_direct():
    await storage.init_db()
    fake_id = uuid4()
    await storage.append_audit(fake_id, "custom_action", "some detail")
    entries = await storage.get_audit_log(fake_id)
    assert len(entries) == 1
    assert entries[0]["action"] == "custom_action"
    assert entries[0]["detail"] == "some detail"
