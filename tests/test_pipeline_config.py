"""Tests for per-request pipeline configuration via the config form field."""
from __future__ import annotations

import json
from pathlib import Path

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


def _cfg(d: dict) -> str:
    return json.dumps(d)


# ---------------------------------------------------------------------------
# force_doc_type
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_force_doc_type_overrides_classifier():
    async with _client() as c:
        r = await c.post(
            "/process",
            files={"file": ("doc.txt", _INVOICE, "text/plain")},
            data={"config": _cfg({"force_doc_type": "contract"})},
        )
    assert r.status_code == 200
    stages = {s["module"]: s for s in r.json()["stages"]}
    assert stages["classifier"]["data"]["type"] == "contract"
    assert stages["classifier"]["data"]["method"] == "forced"
    assert stages["classifier"]["data"]["confidence"] == 1.0


@pytest.mark.asyncio
async def test_force_doc_type_preserved_in_stored_result():
    async with _client() as c:
        r = await c.post(
            "/process",
            files={"file": ("doc.txt", _INVOICE, "text/plain")},
            data={"config": _cfg({"force_doc_type": "report"})},
        )
        doc_id = r.json()["document_id"]
        stored = await c.get(f"/results/{doc_id}")
    stages = {s["module"]: s for s in stored.json()["stages"]}
    assert stages["classifier"]["data"]["type"] == "report"


@pytest.mark.asyncio
async def test_force_doc_type_affects_search_filter():
    async with _client() as c:
        await c.post(
            "/process",
            files={"file": ("doc.txt", _INVOICE, "text/plain")},
            data={"config": _cfg({"force_doc_type": "specification"})},
        )
        r = await c.get("/results?doc_type=specification")
    assert r.json()["total"] == 1


# ---------------------------------------------------------------------------
# llm_enabled override
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_llm_disabled_per_request():
    """With llm_enabled=false the classifier must not call LLM (method != llm)."""
    async with _client() as c:
        r = await c.post(
            "/process",
            files={"file": ("doc.txt", b"mystery content xyz", "text/plain")},
            data={"config": _cfg({"llm_enabled": False})},
        )
    assert r.status_code == 200
    stages = {s["module"]: s for s in r.json()["stages"]}
    assert stages["classifier"]["data"]["method"] != "llm"


# ---------------------------------------------------------------------------
# confidence_threshold override
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_high_confidence_threshold_skips_llm():
    """threshold=1.0 means nothing ever triggers LLM (confidence can't reach 1)."""
    async with _client() as c:
        r = await c.post(
            "/process",
            files={"file": ("doc.txt", _INVOICE, "text/plain")},
            data={"config": _cfg({"confidence_threshold": 1.0})},
        )
    assert r.status_code == 200
    stages = {s["module"]: s for s in r.json()["stages"]}
    assert stages["classifier"]["data"]["method"] != "llm"


# ---------------------------------------------------------------------------
# No config / invalid config — backward compatibility
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_no_config_still_works():
    async with _client() as c:
        r = await c.post(
            "/process",
            files={"file": ("doc.txt", _INVOICE, "text/plain")},
        )
    assert r.status_code == 200
    assert r.json()["status"] in ("success", "partial", "failed")


@pytest.mark.asyncio
async def test_invalid_json_config_ignored():
    """Malformed JSON in config field should not crash the endpoint."""
    async with _client() as c:
        r = await c.post(
            "/process",
            files={"file": ("doc.txt", _INVOICE, "text/plain")},
            data={"config": "not-json{{{"},
        )
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_non_dict_json_config_ignored():
    """A valid JSON array instead of object should be silently ignored."""
    async with _client() as c:
        r = await c.post(
            "/process",
            files={"file": ("doc.txt", _INVOICE, "text/plain")},
            data={"config": "[1, 2, 3]"},
        )
    assert r.status_code == 200


# ---------------------------------------------------------------------------
# stream endpoint honours config too
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_stream_endpoint_force_doc_type():
    async with _client() as c:
        r = await c.post(
            "/process/stream",
            files={"file": ("doc.txt", _INVOICE, "text/plain")},
            data={"config": _cfg({"force_doc_type": "memo"})},
        )
    assert r.status_code == 200
    # SSE stream: find the "done" event and check classifier
    text = r.text
    for line in text.splitlines():
        if line.startswith("data:"):
            payload = json.loads(line[5:].strip())
            if payload.get("type") == "done":
                stages = {s["module"]: s for s in payload["stages"]}
                assert stages["classifier"]["data"]["type"] == "memo"
                assert stages["classifier"]["data"]["method"] == "forced"
                break
