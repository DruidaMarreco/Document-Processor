"""Tests for GET /results/{id}/diff/{other_id}."""
from __future__ import annotations

import json
from pathlib import Path
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient

from document_processor import storage
from document_processor.api import app

_INVOICE = b"Invoice No: 1234\nBill To: John\nAmount Due: $500\nDue Date: 2026-01-01"
_EMAIL = b"From: alice@example.com\nTo: bob@example.com\nSubject: Meeting notes"


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
# Basic structure
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_diff_same_document():
    """Diffing a result against itself should show no changes in top-level fields."""
    async with _client() as c:
        doc_id = await _upload(c, _INVOICE)
        r = await c.get(f"/results/{doc_id}/diff/{doc_id}")
    assert r.status_code == 200
    body = r.json()
    assert "left_id" in body
    assert "right_id" in body
    assert "stages" in body
    assert "top" in body
    assert body["left_id"] == doc_id
    assert body["right_id"] == doc_id
    # Same document — status should be unchanged
    assert body["top"]["status"]["changed"] is False


@pytest.mark.asyncio
async def test_diff_response_structure():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL)
        r = await c.get(f"/results/{id1}/diff/{id2}")
    assert r.status_code == 200
    body = r.json()
    assert "left_id" in body
    assert "right_id" in body
    assert "changed_fields" in body
    assert "top" in body
    assert "stages" in body
    assert isinstance(body["changed_fields"], int)
    assert body["left_id"] == id1
    assert body["right_id"] == id2


@pytest.mark.asyncio
async def test_diff_top_level_fields_present():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL)
        r = await c.get(f"/results/{id1}/diff/{id2}")
    top = r.json()["top"]
    assert "status" in top
    assert "document_id" in top
    assert "total_duration_ms" in top


@pytest.mark.asyncio
async def test_diff_document_ids_always_different():
    """Two different uploads always have different document_ids."""
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL)
        r = await c.get(f"/results/{id1}/diff/{id2}")
    top = r.json()["top"]
    assert top["document_id"]["changed"] is True
    assert top["document_id"]["left"] == id1
    assert top["document_id"]["right"] == id2


@pytest.mark.asyncio
async def test_diff_stages_present():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL)
        r = await c.get(f"/results/{id1}/diff/{id2}")
    stages = r.json()["stages"]
    assert "classifier" in stages
    assert "extractor" in stages
    assert "router" in stages


@pytest.mark.asyncio
async def test_diff_stage_has_status_and_data():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL)
        r = await c.get(f"/results/{id1}/diff/{id2}")
    classifier_diff = r.json()["stages"]["classifier"]
    assert "status" in classifier_diff
    assert "data" in classifier_diff
    assert "changed" in classifier_diff["status"]


@pytest.mark.asyncio
async def test_diff_forced_type_shows_change():
    """Force two different doc types and confirm classifier data shows changed=True."""
    async with _client() as c:
        id1 = await _upload_forced(c, "invoice")
        id2 = await _upload_forced(c, "contract")
        r = await c.get(f"/results/{id1}/diff/{id2}")
    classifier_data = r.json()["stages"]["classifier"]["data"]
    assert classifier_data["type"]["changed"] is True
    assert classifier_data["type"]["left"] == "invoice"
    assert classifier_data["type"]["right"] == "contract"


async def _upload_forced(client, doc_type: str) -> str:
    r = await client.post(
        "/process",
        files={"file": ("doc.txt", _INVOICE, "text/plain")},
        data={"config": json.dumps({"force_doc_type": doc_type})},
    )
    assert r.status_code == 200
    return r.json()["document_id"]


# ---------------------------------------------------------------------------
# 404 handling
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_diff_left_missing_404():
    async with _client() as c:
        id2 = await _upload(c, _INVOICE)
        r = await c.get(f"/results/{uuid4()}/diff/{id2}")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_diff_right_missing_404():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        r = await c.get(f"/results/{id1}/diff/{uuid4()}")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_diff_both_missing_404():
    async with _client() as c:
        r = await c.get(f"/results/{uuid4()}/diff/{uuid4()}")
    assert r.status_code == 404
