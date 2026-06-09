"""Tests for GET /results/{id}/similar — field-token Jaccard similarity search."""
from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient

from document_processor import storage
from document_processor.api import app
from document_processor.storage import _jaccard, _field_tokens

_INVOICE_A = b"Invoice No: 1234\nBill To: John\nAmount Due: $500\nDue Date: 2026-01-01"
_INVOICE_B = b"Invoice No: 5678\nBill To: Jane\nAmount Due: $300\nDue Date: 2026-02-01"
_EMAIL = b"From: alice@example.com\nTo: bob@example.com\nSubject: Meeting notes today"


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
# Unit tests for helpers
# ---------------------------------------------------------------------------

def test_jaccard_identical():
    assert _jaccard({"a", "b", "c"}, {"a", "b", "c"}) == 1.0


def test_jaccard_disjoint():
    assert _jaccard({"a", "b"}, {"c", "d"}) == 0.0


def test_jaccard_partial():
    score = _jaccard({"a", "b", "c"}, {"a", "b", "d"})
    assert abs(score - 0.5) < 1e-9  # |{a,b}| / |{a,b,c,d}| = 2/4


def test_jaccard_empty_both():
    assert _jaccard(set(), set()) == 1.0


def test_jaccard_one_empty():
    assert _jaccard({"a"}, set()) == 0.0


# ---------------------------------------------------------------------------
# API endpoint structure
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_similar_response_structure():
    async with _client() as c:
        doc_id = await _upload(c, _INVOICE_A)
        r = await c.get(f"/results/{doc_id}/similar")
    assert r.status_code == 200
    body = r.json()
    assert "document_id" in body
    assert "similar" in body
    assert body["document_id"] == doc_id
    assert isinstance(body["similar"], list)


@pytest.mark.asyncio
async def test_similar_404_for_missing():
    async with _client() as c:
        r = await c.get(f"/results/{uuid4()}/similar")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_similar_entry_structure():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE_A)
        await _upload(c, _INVOICE_B)
        r = await c.get(f"/results/{id1}/similar")
    similar = r.json()["similar"]
    if similar:
        entry = similar[0]
        assert "document_id" in entry
        assert "score" in entry
        assert "result" in entry
        assert 0.0 <= entry["score"] <= 1.0


@pytest.mark.asyncio
async def test_reference_not_in_similar_results():
    async with _client() as c:
        doc_id = await _upload(c, _INVOICE_A)
        await _upload(c, _INVOICE_B)
        r = await c.get(f"/results/{doc_id}/similar")
    ids = [s["document_id"] for s in r.json()["similar"]]
    assert doc_id not in ids


@pytest.mark.asyncio
async def test_top_n_limit():
    async with _client() as c:
        ref_id = await _upload(c, _INVOICE_A)
        for _ in range(5):
            await _upload(c, _INVOICE_B)
        r = await c.get(f"/results/{ref_id}/similar?top_n=3")
    assert len(r.json()["similar"]) <= 3


@pytest.mark.asyncio
async def test_min_score_filters_low_similarity():
    async with _client() as c:
        ref_id = await _upload(c, _INVOICE_A)
        await _upload(c, _EMAIL)  # very different content
        r = await c.get(f"/results/{ref_id}/similar?min_score=0.99")
    # min_score=0.99 should exclude all but near-identical docs
    for entry in r.json()["similar"]:
        assert entry["score"] >= 0.99


@pytest.mark.asyncio
async def test_scores_descending():
    async with _client() as c:
        ref_id = await _upload(c, _INVOICE_A)
        await _upload(c, _INVOICE_B)
        await _upload(c, _EMAIL)
        r = await c.get(f"/results/{ref_id}/similar")
    scores = [s["score"] for s in r.json()["similar"]]
    assert scores == sorted(scores, reverse=True)


@pytest.mark.asyncio
async def test_similar_with_only_one_result_in_db():
    """No candidates to compare — similar list should be empty."""
    async with _client() as c:
        doc_id = await _upload(c, _INVOICE_A)
        r = await c.get(f"/results/{doc_id}/similar")
    assert r.json()["similar"] == []
