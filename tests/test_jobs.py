"""Tests for the async job queue endpoints."""
from __future__ import annotations

from pathlib import Path
from uuid import UUID, uuid4

import pytest
from httpx import ASGITransport, AsyncClient

from document_processor import storage
from document_processor.api import app
from document_processor.job_worker import _process_job

_INVOICE = b"Invoice No: 1234\nBill To: John\nAmount Due: $500\nDue Date: 2026-01-01"
_CONTRACT = b"This Agreement is entered into by the parties. Whereas hereinafter."


@pytest.fixture(autouse=True)
def tmp_db(tmp_path: Path):
    storage.set_db_path(tmp_path / "test.db")
    yield
    storage.set_db_path(Path("data/results.db"))


def _client():
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


async def _submit_and_process(client: AsyncClient, content: bytes, filename: str) -> dict:
    """Helper: submit a job then synchronously process it, return final job dict."""
    r = await client.post("/jobs", files={"file": (filename, content, "text/plain")})
    assert r.status_code == 202
    job_id = UUID(r.json()["job_id"])
    await _process_job(job_id)
    result = await client.get(f"/jobs/{job_id}")
    return result.json()


# ---------------------------------------------------------------------------
# Job submission
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_submit_job_returns_202_and_queued_status():
    async with _client() as c:
        r = await c.post("/jobs", files={"file": ("inv.txt", _INVOICE, "text/plain")})
    assert r.status_code == 202
    body = r.json()
    assert body["status"] == "queued"
    assert "job_id" in body
    assert body["filename"] == "inv.txt"


@pytest.mark.asyncio
async def test_submit_job_processes_to_done():
    async with _client() as c:
        body = await _submit_and_process(c, _INVOICE, "inv.txt")
    assert body["status"] == "done"
    assert body["result_id"] is not None


@pytest.mark.asyncio
async def test_done_job_result_is_retrievable():
    async with _client() as c:
        job = await _submit_and_process(c, _INVOICE, "inv.txt")
        assert job["status"] == "done"
        result_id = job["result_id"]
        result_r = await c.get(f"/results/{result_id}")
    assert result_r.status_code == 200
    assert result_r.json()["document_id"] == result_id


@pytest.mark.asyncio
async def test_get_job_404_for_unknown():
    async with _client() as c:
        r = await c.get(f"/jobs/{uuid4()}")
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Job listing
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_list_jobs_returns_submitted_jobs():
    async with _client() as c:
        await c.post("/jobs", files={"file": ("a.txt", _INVOICE, "text/plain")})
        await c.post("/jobs", files={"file": ("b.txt", _CONTRACT, "text/plain")})
        r = await c.get("/jobs")
    assert r.status_code == 200
    assert len(r.json()) == 2


@pytest.mark.asyncio
async def test_list_jobs_pagination():
    async with _client() as c:
        for i in range(4):
            await c.post("/jobs", files={"file": (f"f{i}.txt", _INVOICE, "text/plain")})
        r1 = await c.get("/jobs?limit=2&offset=0")
        r2 = await c.get("/jobs?limit=2&offset=2")
    assert len(r1.json()) == 2
    assert len(r2.json()) == 2
    ids1 = {j["job_id"] for j in r1.json()}
    ids2 = {j["job_id"] for j in r2.json()}
    assert ids1.isdisjoint(ids2)


# ---------------------------------------------------------------------------
# Job fields
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_job_has_all_required_fields():
    async with _client() as c:
        r = await c.post("/jobs", files={"file": ("doc.txt", _INVOICE, "text/plain")})
    body = r.json()
    for field in ("job_id", "status", "filename", "mimetype", "created_at"):
        assert field in body, f"missing field: {field}"


@pytest.mark.asyncio
async def test_job_started_and_completed_at_populated_after_done():
    async with _client() as c:
        body = await _submit_and_process(c, _INVOICE, "inv.txt")
    assert body["started_at"] is not None
    assert body["completed_at"] is not None


# ---------------------------------------------------------------------------
# Job status transitions
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_job_status_is_queued_before_processing():
    async with _client() as c:
        r = await c.post("/jobs", files={"file": ("doc.txt", _INVOICE, "text/plain")})
        # Immediately after submit — status should be queued (not yet processed)
        job_r = await c.get(f"/jobs/{r.json()['job_id']}")
    assert job_r.json()["status"] == "queued"


@pytest.mark.asyncio
async def test_failed_job_when_document_missing():
    """Simulate a job whose document bytes were deleted before processing."""
    import aiosqlite
    async with _client() as c:
        r = await c.post("/jobs", files={"file": ("doc.txt", _INVOICE, "text/plain")})
    job_id = UUID(r.json()["job_id"])
    # Delete just the document bytes to force a "not found" failure in the worker
    async with aiosqlite.connect(storage._db_path()) as db:
        await db.execute("DELETE FROM documents WHERE id = ?", (str(job_id),))
        await db.commit()
    await _process_job(job_id)
    job = await storage.get_job(job_id)
    assert job is not None
    assert job.status == "failed"
    assert job.error is not None


# ---------------------------------------------------------------------------
# Multiple jobs
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_multiple_jobs_all_complete():
    async with _client() as c:
        job_ids = []
        for i in range(3):
            r = await c.post("/jobs", files={"file": (f"f{i}.txt", _INVOICE, "text/plain")})
            job_ids.append(UUID(r.json()["job_id"]))

        for jid in job_ids:
            await _process_job(jid)

        statuses = []
        for jid in job_ids:
            jr = await c.get(f"/jobs/{jid}")
            statuses.append(jr.json()["status"])

    assert all(s == "done" for s in statuses)
