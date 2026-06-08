"""Tests for /batch endpoint and /process/stream SSE endpoint."""
from __future__ import annotations

import json

import pytest
from httpx import ASGITransport, AsyncClient

from document_processor.api import app


@pytest.mark.asyncio
async def test_batch_returns_multiple_results():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        files = [
            ("files", ("a.txt", b"Invoice No: 1 Amount Due: $100 Due Date: 2026-01-01", "text/plain")),
            ("files", ("b.txt", b"Contract Agreement between parties hereinafter", "text/plain")),
        ]
        r = await client.post("/batch", files=files)
    assert r.status_code == 200
    results = r.json()
    assert len(results) == 2
    assert all("status" in res for res in results)


@pytest.mark.asyncio
async def test_batch_empty_files_rejected():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.post("/batch", files=[])
    assert r.status_code in (400, 422)


@pytest.mark.asyncio
async def test_batch_too_many_files_rejected():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        files = [
            ("files", (f"f{i}.txt", b"content", "text/plain"))
            for i in range(21)
        ]
        r = await client.post("/batch", files=files)
    assert r.status_code == 400


@pytest.mark.asyncio
async def test_batch_partial_failure_returns_error_entry():
    """A bad file doesn't crash the whole batch — returns an error entry."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        files = [
            ("files", ("good.txt", b"Invoice No: 1 Amount Due: $50", "text/plain")),
            ("files", ("bad.bin", b"\x00" * 10, "application/octet-stream")),
        ]
        r = await client.post("/batch", files=files)
    assert r.status_code == 200
    results = r.json()
    assert len(results) == 2


@pytest.mark.asyncio
async def test_process_stream_yields_stages_and_done():
    """SSE stream yields one event per stage then a 'done' event."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.post(
            "/process/stream",
            files={"file": ("test.txt", b"Invoice No: 123 Amount Due: $200", "text/plain")},
        )
    assert r.status_code == 200
    assert "text/event-stream" in r.headers["content-type"]

    events = []
    for line in r.text.splitlines():
        if line.startswith("data: "):
            events.append(json.loads(line[6:]))

    types = [e["type"] for e in events]
    assert "stage" in types
    assert types[-1] == "done"
    stage_events = [e for e in events if e["type"] == "stage"]
    assert len(stage_events) == 6  # router, classifier, extractor, refiner, validator, generator

    done = events[-1]
    assert "document_id" in done
    assert done["status"] in ("success", "partial", "failed")


@pytest.mark.asyncio
async def test_upload_page_served():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get("/upload")
    assert r.status_code == 200
    assert "text/html" in r.headers["content-type"]


@pytest.mark.asyncio
async def test_root_serves_upload_page():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get("/")
    assert r.status_code == 200
    assert "text/html" in r.headers["content-type"]
