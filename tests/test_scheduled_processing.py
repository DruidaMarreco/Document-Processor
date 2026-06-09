"""Tests for POST /schedule — scheduled document processing."""
from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient

from document_processor import storage
from document_processor.api import app

_INVOICE = b"Invoice No: 1234\nBill To: John\nAmount Due: $500\nDue Date: 2026-01-01"

_FUTURE = "2030-01-01T00:00:00Z"
_PAST   = "2000-01-01T00:00:00Z"


@pytest.fixture(autouse=True)
def tmp_db(tmp_path: Path):
    storage.set_db_path(tmp_path / "test.db")
    yield
    storage.set_db_path(Path("data/results.db"))


def _client():
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


async def _schedule(client, run_at: str = _FUTURE) -> str:
    r = await client.post(
        f"/schedule?run_at={run_at}",
        files={"file": ("invoice.txt", _INVOICE, "text/plain")},
    )
    assert r.status_code == 201
    return r.json()["id"]


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_schedule_creates_entry():
    async with _client() as c:
        r = await c.post(
            f"/schedule?run_at={_FUTURE}",
            files={"file": ("doc.txt", _INVOICE, "text/plain")},
        )
    assert r.status_code == 201
    body = r.json()
    assert "id" in body
    assert body["run_at"] == _FUTURE
    assert body["status"] == "pending"
    assert body["filename"] == "doc.txt"


@pytest.mark.asyncio
async def test_get_scheduled_job():
    async with _client() as c:
        sched_id = await _schedule(c)
        r = await c.get(f"/schedule/{sched_id}")
    assert r.status_code == 200
    assert r.json()["id"] == sched_id
    assert r.json()["status"] == "pending"


@pytest.mark.asyncio
async def test_get_missing_scheduled_404():
    async with _client() as c:
        r = await c.get(f"/schedule/{uuid4()}")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_list_scheduled_jobs():
    async with _client() as c:
        id1 = await _schedule(c)
        id2 = await _schedule(c)
        r = await c.get("/schedule")
    ids = [j["id"] for j in r.json()]
    assert id1 in ids
    assert id2 in ids


@pytest.mark.asyncio
async def test_list_scheduled_filter_by_status():
    async with _client() as c:
        sched_id = await _schedule(c)
        r_pending = await c.get("/schedule?status=pending")
        r_dispatched = await c.get("/schedule?status=dispatched")
    pending_ids = [j["id"] for j in r_pending.json()]
    assert sched_id in pending_ids
    assert r_dispatched.json() == []


@pytest.mark.asyncio
async def test_cancel_pending_job():
    async with _client() as c:
        sched_id = await _schedule(c)
        r_del = await c.delete(f"/schedule/{sched_id}")
        r_get = await c.get(f"/schedule/{sched_id}")
    assert r_del.status_code == 204
    assert r_get.status_code == 404


@pytest.mark.asyncio
async def test_cancel_missing_job_404():
    async with _client() as c:
        r = await c.delete(f"/schedule/{uuid4()}")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_document_stored_at_schedule_time():
    """The uploaded bytes are stored immediately (not deferred) for later retrieval."""
    async with _client() as c:
        sched_id = await _schedule(c, run_at=_FUTURE)
    doc = await storage.get_document(sched_id)
    assert doc is not None
    assert doc.content == _INVOICE


# ---------------------------------------------------------------------------
# Dispatch logic (via storage.claim_due_scheduled_jobs)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_claim_due_returns_past_jobs():
    await storage.init_db()
    from document_processor.models import Document as Doc
    doc = Doc(filename="f.txt", mimetype="text/plain", content=_INVOICE)
    sched_id = str(uuid4())
    await storage.create_scheduled_job(sched_id, _PAST, doc)
    now = "2026-06-09T12:00:00Z"
    claimed = await storage.claim_due_scheduled_jobs(now)
    assert any(j["id"] == sched_id for j in claimed)


@pytest.mark.asyncio
async def test_claim_due_skips_future_jobs():
    await storage.init_db()
    from document_processor.models import Document as Doc
    doc = Doc(filename="f.txt", mimetype="text/plain", content=_INVOICE)
    sched_id = str(uuid4())
    await storage.create_scheduled_job(sched_id, _FUTURE, doc)
    now = "2026-06-09T12:00:00Z"
    claimed = await storage.claim_due_scheduled_jobs(now)
    assert not any(j["id"] == sched_id for j in claimed)


@pytest.mark.asyncio
async def test_claim_due_is_idempotent():
    """Claiming the same job twice should not return it twice."""
    await storage.init_db()
    from document_processor.models import Document as Doc
    doc = Doc(filename="f.txt", mimetype="text/plain", content=_INVOICE)
    sched_id = str(uuid4())
    await storage.create_scheduled_job(sched_id, _PAST, doc)
    now = "2026-06-09T12:00:00Z"
    first = await storage.claim_due_scheduled_jobs(now)
    second = await storage.claim_due_scheduled_jobs(now)
    assert any(j["id"] == sched_id for j in first)
    assert not any(j["id"] == sched_id for j in second)


@pytest.mark.asyncio
async def test_cancel_dispatched_job_returns_409():
    """Cannot cancel a job that has already been dispatched."""
    await storage.init_db()
    from document_processor.models import Document as Doc
    doc = Doc(filename="f.txt", mimetype="text/plain", content=_INVOICE)
    sched_id = str(uuid4())
    await storage.create_scheduled_job(sched_id, _PAST, doc)
    await storage.update_scheduled_job_status(sched_id, "dispatched")
    async with _client() as c:
        r = await c.delete(f"/schedule/{sched_id}")
    assert r.status_code == 409
