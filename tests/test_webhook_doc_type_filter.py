"""Tests for webhook doc_types filter."""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from document_processor import storage
from document_processor.api import app
from document_processor.models import PipelineResult, StageResult, WebhookConfig
from document_processor.webhook_delivery import fire_webhooks

_INVOICE = b"Invoice No: 1234\nBill To: John\nAmount Due: $500\nDue Date: 2026-01-01"


@pytest.fixture(autouse=True)
def tmp_db(tmp_path: Path):
    storage.set_db_path(tmp_path / "test.db")
    yield
    storage.set_db_path(Path("data/results.db"))


def _client():
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


def _make_result(doc_type: str) -> PipelineResult:
    return PipelineResult(
        document_id=__import__("uuid").uuid4(),
        status="success",
        total_duration_ms=10.0,
        stages=[
            StageResult(module="classifier", status="success",
                        data={"type": doc_type, "confidence": 0.9}, duration_ms=1.0),
        ],
    )


# ---------------------------------------------------------------------------
# Model / storage
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_webhook_created_with_doc_types():
    async with _client() as c:
        r = await c.post("/webhooks", json={
            "url": "https://example.com/hook",
            "doc_types": ["invoice", "contract"],
        })
    assert r.status_code == 201
    assert r.json()["doc_types"] == ["invoice", "contract"]


@pytest.mark.asyncio
async def test_webhook_created_without_doc_types_defaults_empty():
    async with _client() as c:
        r = await c.post("/webhooks", json={"url": "https://example.com/hook"})
    assert r.status_code == 201
    assert r.json()["doc_types"] == []


@pytest.mark.asyncio
async def test_webhook_doc_types_persisted_and_retrieved():
    async with _client() as c:
        r = await c.post("/webhooks", json={
            "url": "https://example.com/hook",
            "doc_types": ["receipt"],
        })
        wh_id = r.json()["id"]
        webhooks = await c.get("/webhooks")
    wh = next(w for w in webhooks.json() if w["id"] == wh_id)
    assert wh["doc_types"] == ["receipt"]


# ---------------------------------------------------------------------------
# fire_webhooks filtering
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_no_doc_types_filter_fires_for_any_type():
    """Empty doc_types = no filter: fires for any doc_type."""
    wh = WebhookConfig(url="https://example.com/hook", doc_types=[])
    result = _make_result("invoice")
    delivered: list = []

    async def fake_deliver(w, event, payload):
        delivered.append(w.url)
        return True

    with patch("document_processor.webhook_delivery._deliver_one", new=fake_deliver):
        await fire_webhooks([wh], "document.processed", result)

    assert len(delivered) == 1


@pytest.mark.asyncio
async def test_doc_types_filter_matches_type():
    wh = WebhookConfig(url="https://example.com/hook", doc_types=["invoice", "contract"])
    result = _make_result("invoice")
    delivered: list = []

    async def fake_deliver(w, event, payload):
        delivered.append(w.url)
        return True

    with patch("document_processor.webhook_delivery._deliver_one", new=fake_deliver):
        await fire_webhooks([wh], "document.processed", result)

    assert len(delivered) == 1


@pytest.mark.asyncio
async def test_doc_types_filter_excludes_non_matching_type():
    wh = WebhookConfig(url="https://example.com/hook", doc_types=["contract"])
    result = _make_result("invoice")
    delivered: list = []

    async def fake_deliver(w, event, payload):
        delivered.append(w.url)
        return True

    with patch("document_processor.webhook_delivery._deliver_one", new=fake_deliver):
        await fire_webhooks([wh], "document.processed", result)

    assert len(delivered) == 0


@pytest.mark.asyncio
async def test_multiple_webhooks_selective_delivery():
    """One webhook matches doc_type, another doesn't."""
    wh_all = WebhookConfig(url="https://a.com", doc_types=[])
    wh_contract = WebhookConfig(url="https://b.com", doc_types=["contract"])
    result = _make_result("invoice")
    delivered: list = []

    async def fake_deliver(w, event, payload):
        delivered.append(w.url)
        return True

    with patch("document_processor.webhook_delivery._deliver_one", new=fake_deliver):
        await fire_webhooks([wh_all, wh_contract], "document.processed", result)

    assert "https://a.com" in delivered
    assert "https://b.com" not in delivered


@pytest.mark.asyncio
async def test_doc_type_filter_with_none_doc_type():
    """If classifier stage is absent, doc_type is None — filter should exclude it."""
    wh = WebhookConfig(url="https://example.com/hook", doc_types=["invoice"])
    result = PipelineResult(
        document_id=__import__("uuid").uuid4(),
        status="success",
        total_duration_ms=5.0,
        stages=[],  # no classifier stage → doc_type is None
    )
    delivered: list = []

    async def fake_deliver(w, event, payload):
        delivered.append(w.url)
        return True

    with patch("document_processor.webhook_delivery._deliver_one", new=fake_deliver):
        await fire_webhooks([wh], "document.processed", result)

    assert len(delivered) == 0
