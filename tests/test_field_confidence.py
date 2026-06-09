"""Tests for field-level confidence scoring and GET /results/{id}/confidence."""
from __future__ import annotations

import json
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


async def _upload(client, content: bytes = _INVOICE, config: dict | None = None) -> str:
    data = {}
    if config:
        data["config"] = json.dumps(config)
    r = await client.post(
        "/process",
        files={"file": ("doc.txt", content, "text/plain")},
        data=data,
    )
    assert r.status_code == 200
    return r.json()["document_id"]


# ---------------------------------------------------------------------------
# Endpoint structure
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_confidence_endpoint_structure():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.get(f"/results/{doc_id}/confidence")
    assert r.status_code == 200
    body = r.json()
    assert "document_id" in body
    assert "overall_confidence" in body
    assert "field_confidence" in body
    assert "field_count" in body
    assert body["document_id"] == doc_id


@pytest.mark.asyncio
async def test_confidence_endpoint_404_for_missing():
    async with _client() as c:
        r = await c.get(f"/results/{uuid4()}/confidence")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_overall_confidence_is_float_or_none():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.get(f"/results/{doc_id}/confidence")
    oc = r.json()["overall_confidence"]
    assert oc is None or isinstance(oc, float)


@pytest.mark.asyncio
async def test_field_confidence_is_dict():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.get(f"/results/{doc_id}/confidence")
    fc = r.json()["field_confidence"]
    assert isinstance(fc, dict)


@pytest.mark.asyncio
async def test_field_count_matches_field_confidence():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.get(f"/results/{doc_id}/confidence")
    body = r.json()
    assert body["field_count"] == len(body["field_confidence"])


# ---------------------------------------------------------------------------
# Score properties
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_field_confidence_scores_between_0_and_1():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.get(f"/results/{doc_id}/confidence")
    for field, score in r.json()["field_confidence"].items():
        assert 0.0 <= score <= 1.0, f"{field} score {score} out of range"


@pytest.mark.asyncio
async def test_forced_type_gives_high_confidence():
    """force_doc_type uses method='forced' which adds a bonus."""
    async with _client() as c:
        doc_id = await _upload(c, config={"force_doc_type": "invoice"})
        r_normal = await _upload(c)
        conf_forced = (await c.get(f"/results/{doc_id}/confidence")).json()
        conf_normal = (await c.get(f"/results/{r_normal}/confidence")).json()
    # forced overall confidence is 1.0; normal confidence depends on heuristic
    assert conf_forced["overall_confidence"] == 1.0


@pytest.mark.asyncio
async def test_field_confidence_included_in_result_output():
    """field_confidence should also appear in the generator stage output."""
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.get(f"/results/{doc_id}")
    generator_stage = next(
        (s for s in r.json()["stages"] if s["module"] == "generator"), None
    )
    assert generator_stage is not None
    output = generator_stage["data"].get("output", {})
    assert "field_confidence" in output


# ---------------------------------------------------------------------------
# Generator unit: _field_confidence helper
# ---------------------------------------------------------------------------

def test_field_confidence_helper_no_fields():
    from generator_module.generator import _field_confidence
    ctx = {"classifier": {"confidence": 0.8, "method": "keyword"}, "refiner": {"fields": {}}}
    assert _field_confidence(ctx) == {}


def test_field_confidence_helper_basic():
    from generator_module.generator import _field_confidence
    ctx = {
        "classifier": {"confidence": 0.8, "method": "keyword"},
        "refiner": {"fields": {"amount": ["$500"], "date": ["2026-01-01"]}},
        "validator": {"violations": []},
    }
    scores = _field_confidence(ctx)
    assert set(scores) == {"amount", "date"}
    for v in scores.values():
        assert 0.0 <= v <= 1.0


def test_field_confidence_helper_forced_method():
    from generator_module.generator import _field_confidence
    ctx = {
        "classifier": {"confidence": 1.0, "method": "forced"},
        "refiner": {"fields": {"invoice_number": ["1234"]}},
        "validator": {"violations": []},
    }
    score = _field_confidence(ctx)["invoice_number"]
    assert score == 1.0  # 1.0 + 0.15 bonus, capped at 1.0
