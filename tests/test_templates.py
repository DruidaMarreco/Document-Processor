"""Tests for processing templates (POST/GET/DELETE /templates, ?template= param)."""
from __future__ import annotations

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


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_create_template():
    async with _client() as c:
        r = await c.post("/templates", json={
            "name": "invoice-strict",
            "config": {"force_doc_type": "invoice", "llm_enabled": False},
            "description": "Force invoice, no LLM",
        })
    assert r.status_code == 201
    body = r.json()
    assert body["name"] == "invoice-strict"
    assert body["config"]["force_doc_type"] == "invoice"
    assert body["description"] == "Force invoice, no LLM"
    assert "created_at" in body
    assert "updated_at" in body


@pytest.mark.asyncio
async def test_create_template_no_description():
    async with _client() as c:
        r = await c.post("/templates", json={"name": "bare", "config": {"llm_enabled": False}})
    assert r.status_code == 201
    assert r.json()["description"] is None


@pytest.mark.asyncio
async def test_get_template():
    async with _client() as c:
        await c.post("/templates", json={"name": "t1", "config": {"confidence_threshold": 0.8}})
        r = await c.get("/templates/t1")
    assert r.status_code == 200
    assert r.json()["config"]["confidence_threshold"] == 0.8


@pytest.mark.asyncio
async def test_get_missing_template_404():
    async with _client() as c:
        r = await c.get("/templates/nonexistent")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_list_templates():
    async with _client() as c:
        await c.post("/templates", json={"name": "a", "config": {}})
        await c.post("/templates", json={"name": "b", "config": {}})
        r = await c.get("/templates")
    names = [t["name"] for t in r.json()]
    assert "a" in names
    assert "b" in names


@pytest.mark.asyncio
async def test_template_upsert():
    """POSTing a template with the same name replaces its config."""
    async with _client() as c:
        await c.post("/templates", json={"name": "t", "config": {"llm_enabled": True}})
        await c.post("/templates", json={"name": "t", "config": {"llm_enabled": False}})
        r = await c.get("/templates/t")
    assert r.json()["config"]["llm_enabled"] is False


@pytest.mark.asyncio
async def test_delete_template():
    async with _client() as c:
        await c.post("/templates", json={"name": "del-me", "config": {}})
        r_del = await c.delete("/templates/del-me")
        r_get = await c.get("/templates/del-me")
    assert r_del.status_code == 204
    assert r_get.status_code == 404


@pytest.mark.asyncio
async def test_delete_missing_template_404():
    async with _client() as c:
        r = await c.delete("/templates/ghost")
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Template applied at process time
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_process_with_template():
    async with _client() as c:
        await c.post("/templates", json={
            "name": "force-contract",
            "config": {"force_doc_type": "contract"},
        })
        r = await c.post(
            "/process?template=force-contract",
            files={"file": ("doc.txt", _INVOICE, "text/plain")},
        )
    assert r.status_code == 200
    stages = {s["module"]: s for s in r.json()["stages"]}
    assert stages["classifier"]["data"]["type"] == "contract"
    assert stages["classifier"]["data"]["method"] == "forced"


@pytest.mark.asyncio
async def test_process_unknown_template_falls_back_gracefully():
    """An unknown template name is silently ignored — pipeline runs normally."""
    async with _client() as c:
        r = await c.post(
            "/process?template=nonexistent",
            files={"file": ("doc.txt", _INVOICE, "text/plain")},
        )
    assert r.status_code == 200
    assert r.json()["status"] in ("success", "partial", "failed")


@pytest.mark.asyncio
async def test_inline_config_overrides_template():
    """Inline config takes priority over template config for the same key."""
    import json
    async with _client() as c:
        await c.post("/templates", json={
            "name": "force-invoice",
            "config": {"force_doc_type": "invoice"},
        })
        r = await c.post(
            "/process?template=force-invoice",
            files={"file": ("doc.txt", _INVOICE, "text/plain")},
            data={"config": json.dumps({"force_doc_type": "memo"})},
        )
    stages = {s["module"]: s for s in r.json()["stages"]}
    assert stages["classifier"]["data"]["type"] == "memo"


@pytest.mark.asyncio
async def test_template_config_applied_without_inline():
    """Template config is used when no inline config is provided."""
    async with _client() as c:
        await c.post("/templates", json={
            "name": "no-llm",
            "config": {"llm_enabled": False},
        })
        r = await c.post(
            "/process?template=no-llm",
            files={"file": ("doc.txt", b"mystery content xyz", "text/plain")},
        )
    assert r.status_code == 200
    stages = {s["module"]: s for s in r.json()["stages"]}
    assert stages["classifier"]["data"]["method"] != "llm"
