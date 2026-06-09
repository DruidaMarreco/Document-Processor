"""Tests for result workflow status — GET/PUT/DELETE /results/{id}/workflow."""
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


async def _upload(client, content: bytes = _INVOICE) -> str:
    r = await client.post("/process", files={"file": ("doc.txt", content, "text/plain")})
    assert r.status_code == 200
    return r.json()["document_id"]


# ---------------------------------------------------------------------------
# Storage unit tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_workflow_status_initially_none():
    async with _client() as c:
        doc_id = await _upload(c)
    ws = await storage.get_workflow_status(__import__("uuid").UUID(doc_id))
    assert ws is None


@pytest.mark.asyncio
async def test_set_and_get_workflow_status():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    updated = await storage.set_workflow_status(uid, "approved")
    assert updated is True
    ws = await storage.get_workflow_status(uid)
    assert ws == "approved"


@pytest.mark.asyncio
async def test_set_workflow_status_on_missing_returns_false():
    updated = await storage.set_workflow_status(uuid4(), "approved")
    assert updated is False


@pytest.mark.asyncio
async def test_workflow_status_can_be_changed():
    async with _client() as c:
        doc_id = await _upload(c)
    uid = __import__("uuid").UUID(doc_id)
    await storage.set_workflow_status(uid, "pending_review")
    await storage.set_workflow_status(uid, "approved")
    ws = await storage.get_workflow_status(uid)
    assert ws == "approved"


@pytest.mark.asyncio
async def test_workflow_status_filter_in_search():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL)
    await storage.set_workflow_status(__import__("uuid").UUID(id1), "approved")
    results, total = await storage.search_results(workflow_status="approved")
    assert total == 1
    assert str(results[0].document_id) == id1


@pytest.mark.asyncio
async def test_workflow_status_filter_excludes_unset():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        await _upload(c, _EMAIL)
    await storage.set_workflow_status(__import__("uuid").UUID(id1), "rejected")
    results, total = await storage.search_results(workflow_status="pending_review")
    assert total == 0


# ---------------------------------------------------------------------------
# API endpoint tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_workflow_status_initially_null():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.get(f"/results/{doc_id}/workflow")
    assert r.status_code == 200
    assert r.json()["workflow_status"] is None
    assert r.json()["document_id"] == doc_id


@pytest.mark.asyncio
async def test_get_workflow_status_404_for_missing():
    async with _client() as c:
        r = await c.get(f"/results/{uuid4()}/workflow")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_set_workflow_status_204():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.put(f"/results/{doc_id}/workflow",
                        json={"status": "pending_review"})
    assert r.status_code == 204


@pytest.mark.asyncio
async def test_set_workflow_status_reflected_in_get():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.put(f"/results/{doc_id}/workflow", json={"status": "approved"})
        r = await c.get(f"/results/{doc_id}/workflow")
    assert r.json()["workflow_status"] == "approved"


@pytest.mark.asyncio
async def test_set_invalid_workflow_status_400():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.put(f"/results/{doc_id}/workflow", json={"status": "invalid_state"})
    assert r.status_code == 400


@pytest.mark.asyncio
async def test_set_workflow_status_404_for_missing():
    async with _client() as c:
        r = await c.put(f"/results/{uuid4()}/workflow", json={"status": "approved"})
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_all_allowed_statuses_accepted():
    async with _client() as c:
        doc_id = await _upload(c)
        for status in ("pending_review", "approved", "rejected", "archived"):
            r = await c.put(f"/results/{doc_id}/workflow", json={"status": status})
            assert r.status_code == 204, f"status={status} was rejected"


@pytest.mark.asyncio
async def test_clear_workflow_status_204():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.put(f"/results/{doc_id}/workflow", json={"status": "approved"})
        r = await c.delete(f"/results/{doc_id}/workflow")
    assert r.status_code == 204


@pytest.mark.asyncio
async def test_clear_workflow_status_sets_null():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.put(f"/results/{doc_id}/workflow", json={"status": "approved"})
        await c.delete(f"/results/{doc_id}/workflow")
        r = await c.get(f"/results/{doc_id}/workflow")
    assert r.json()["workflow_status"] is None


@pytest.mark.asyncio
async def test_clear_workflow_status_404_for_missing():
    async with _client() as c:
        r = await c.delete(f"/results/{uuid4()}/workflow")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_filter_by_workflow_status_via_list_endpoint():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL)
        await c.put(f"/results/{id1}/workflow", json={"status": "approved"})
        r = await c.get("/results?workflow_status=approved")
    body = r.json()
    assert body["total"] == 1
    assert body["results"][0]["document_id"] == id1


@pytest.mark.asyncio
async def test_workflow_status_change_in_audit_log():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.put(f"/results/{doc_id}/workflow", json={"status": "rejected"})
        r = await c.get(f"/results/{doc_id}/audit")
    actions = [e["action"] for e in r.json()["entries"]]
    assert "workflow_status_set" in actions
