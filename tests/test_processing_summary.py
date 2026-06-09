"""Tests for GET /results/stats/summary — rich aggregate dashboard endpoint."""
from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient

from document_processor import storage
from document_processor.api import app

_INVOICE = b"Invoice No: 1234\nBill To: John\nAmount Due: $500\nDue Date: 2026-01-01"
_EMAIL = b"From: alice@example.com\nTo: bob@example.com\nSubject: Meeting"

_PAST = "2000-01-01T00:00:00Z"
_FUTURE = "2099-01-01T00:00:00Z"


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
# Storage unit tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_summary_zero_when_empty():
    s = await storage.get_processing_summary()
    assert s["total"] == 0
    assert s["flags"]["starred"] == 0
    assert s["flags"]["pinned"] == 0
    assert s["flags"]["locked"] == 0


@pytest.mark.asyncio
async def test_summary_total_count():
    async with _client() as c:
        await _upload(c, _INVOICE)
        await _upload(c, _EMAIL, "email.txt")
    s = await storage.get_processing_summary()
    assert s["total"] == 2


@pytest.mark.asyncio
async def test_summary_flag_counts():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL, "email.txt")
    uid1 = __import__("uuid").UUID(id1)
    uid2 = __import__("uuid").UUID(id2)
    await storage.set_starred(uid1, True)
    await storage.set_pinned(uid1, True)
    await storage.set_result_locked(uid2, True)
    s = await storage.get_processing_summary()
    assert s["flags"]["starred"] == 1
    assert s["flags"]["pinned"] == 1
    assert s["flags"]["locked"] == 1


@pytest.mark.asyncio
async def test_summary_priority_breakdown():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL, "email.txt")
    await storage.set_result_priority(__import__("uuid").UUID(id1), "high")
    await storage.set_result_priority(__import__("uuid").UUID(id2), "critical")
    s = await storage.get_processing_summary()
    assert s["by_priority"].get("high") == 1
    assert s["by_priority"].get("critical") == 1


@pytest.mark.asyncio
async def test_summary_workflow_breakdown():
    async with _client() as c:
        id1 = await _upload(c)
    await storage.set_workflow_status(__import__("uuid").UUID(id1), "approved")
    s = await storage.get_processing_summary()
    assert s["by_workflow_status"].get("approved") == 1


@pytest.mark.asyncio
async def test_summary_expired_count():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL, "email.txt")
    await storage.set_result_expiry(__import__("uuid").UUID(id1), _PAST)
    s = await storage.get_processing_summary()
    assert s["expired"] == 1


@pytest.mark.asyncio
async def test_summary_top_tags():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL, "email.txt")
    uid1 = __import__("uuid").UUID(id1)
    uid2 = __import__("uuid").UUID(id2)
    await storage.add_tags(uid1, ["important", "review"])
    await storage.add_tags(uid2, ["important"])
    s = await storage.get_processing_summary()
    top_tag_names = [t["tag"] for t in s["top_tags"]]
    assert "important" in top_tag_names


@pytest.mark.asyncio
async def test_summary_top_labels():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
    uid1 = __import__("uuid").UUID(id1)
    await storage.create_label("urgent", "#ff0000")
    await storage.apply_label(uid1, "urgent")
    s = await storage.get_processing_summary()
    label_names = [l["name"] for l in s["top_labels"]]
    assert "urgent" in label_names


@pytest.mark.asyncio
async def test_summary_processed_windows_present():
    s = await storage.get_processing_summary()
    assert "last_24h" in s["processed_windows"]
    assert "last_7d" in s["processed_windows"]
    assert "last_30d" in s["processed_windows"]


@pytest.mark.asyncio
async def test_summary_processed_windows_counts():
    async with _client() as c:
        await _upload(c)
    s = await storage.get_processing_summary()
    # Freshly uploaded docs appear in all windows
    assert s["processed_windows"]["last_24h"] >= 1
    assert s["processed_windows"]["last_7d"] >= 1
    assert s["processed_windows"]["last_30d"] >= 1


# ---------------------------------------------------------------------------
# API endpoint tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_summary_endpoint_200():
    async with _client() as c:
        r = await c.get("/results/stats/summary")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_summary_response_structure():
    async with _client() as c:
        r = await c.get("/results/stats/summary")
    body = r.json()
    for field in ("total", "expired", "flags", "by_priority", "by_workflow_status",
                  "top_tags", "top_labels", "processed_windows"):
        assert field in body


@pytest.mark.asyncio
async def test_summary_flags_structure():
    async with _client() as c:
        r = await c.get("/results/stats/summary")
    flags = r.json()["flags"]
    assert "starred" in flags
    assert "pinned" in flags
    assert "locked" in flags


@pytest.mark.asyncio
async def test_summary_processed_windows_structure():
    async with _client() as c:
        r = await c.get("/results/stats/summary")
    windows = r.json()["processed_windows"]
    assert "last_24h" in windows
    assert "last_7d" in windows
    assert "last_30d" in windows


@pytest.mark.asyncio
async def test_summary_top_tags_list():
    async with _client() as c:
        r = await c.get("/results/stats/summary")
    assert isinstance(r.json()["top_tags"], list)


@pytest.mark.asyncio
async def test_summary_top_labels_list():
    async with _client() as c:
        r = await c.get("/results/stats/summary")
    assert isinstance(r.json()["top_labels"], list)


@pytest.mark.asyncio
async def test_summary_reflects_uploaded_docs():
    async with _client() as c:
        await _upload(c, _INVOICE)
        await _upload(c, _EMAIL, "email.txt")
        r = await c.get("/results/stats/summary")
    assert r.json()["total"] == 2


@pytest.mark.asyncio
async def test_summary_reflects_starred():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.put(f"/results/{doc_id}/star")
        r = await c.get("/results/stats/summary")
    assert r.json()["flags"]["starred"] == 1


@pytest.mark.asyncio
async def test_summary_reflects_priority():
    async with _client() as c:
        doc_id = await _upload(c)
        await c.put(f"/results/{doc_id}/priority", json={"priority": "critical"})
        r = await c.get("/results/stats/summary")
    assert r.json()["by_priority"].get("critical") == 1
