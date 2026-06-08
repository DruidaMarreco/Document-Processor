"""Tests for webhook CRUD, delivery, and result-stats endpoints."""
from __future__ import annotations

import hashlib
import hmac
import json
from pathlib import Path
from uuid import uuid4

import pytest
import respx
from httpx import ASGITransport, AsyncClient, Response

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


# ---------------------------------------------------------------------------
# Webhook CRUD
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_create_webhook_returns_201():
    async with _client() as c:
        r = await c.post("/webhooks", json={"url": "https://example.com/hook"})
    assert r.status_code == 201
    body = r.json()
    assert body["url"] == "https://example.com/hook"
    assert "id" in body
    assert "document.processed" in body["events"]


@pytest.mark.asyncio
async def test_create_webhook_custom_events():
    async with _client() as c:
        r = await c.post("/webhooks", json={
            "url": "https://example.com/hook",
            "events": ["document.failed"],
            "secret": "my-secret",
        })
    assert r.status_code == 201
    body = r.json()
    assert body["events"] == ["document.failed"]
    assert body["secret"] == "my-secret"


@pytest.mark.asyncio
async def test_list_webhooks_returns_registered():
    async with _client() as c:
        await c.post("/webhooks", json={"url": "https://a.example.com/hook"})
        await c.post("/webhooks", json={"url": "https://b.example.com/hook"})
        r = await c.get("/webhooks")
    assert r.status_code == 200
    urls = {wh["url"] for wh in r.json()}
    assert "https://a.example.com/hook" in urls
    assert "https://b.example.com/hook" in urls


@pytest.mark.asyncio
async def test_delete_webhook_removes_it():
    async with _client() as c:
        create_r = await c.post("/webhooks", json={"url": "https://example.com/hook"})
        wh_id = create_r.json()["id"]
        del_r = await c.delete(f"/webhooks/{wh_id}")
        list_r = await c.get("/webhooks")
    assert del_r.status_code == 204
    assert all(wh["id"] != wh_id for wh in list_r.json())


@pytest.mark.asyncio
async def test_delete_missing_webhook_404():
    async with _client() as c:
        r = await c.delete(f"/webhooks/{uuid4()}")
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Webhook delivery unit tests
# ---------------------------------------------------------------------------

def _make_result(status: str = "success") -> PipelineResult:
    return PipelineResult(
        document_id=uuid4(),
        status=status,
        stages=[StageResult(module="router", status="success", duration_ms=1.0)],
        output={},
        total_duration_ms=1.0,
    )


@pytest.mark.asyncio
async def test_fire_webhooks_posts_to_url():
    result = _make_result()
    wh = WebhookConfig(url="https://hook.example.com/recv", events=["document.processed"])
    with respx.mock:
        route = respx.post("https://hook.example.com/recv").mock(return_value=Response(200))
        await fire_webhooks([wh], "document.processed", result)
    assert route.called


@pytest.mark.asyncio
async def test_fire_webhooks_skips_non_matching_event():
    result = _make_result()
    wh = WebhookConfig(url="https://hook.example.com/recv", events=["document.failed"])
    with respx.mock:
        route = respx.post("https://hook.example.com/recv").mock(return_value=Response(200))
        await fire_webhooks([wh], "document.processed", result)
    assert not route.called


@pytest.mark.asyncio
async def test_fire_webhooks_skips_inactive():
    result = _make_result()
    wh = WebhookConfig(url="https://hook.example.com/recv", active=False,
                       events=["document.processed"])
    with respx.mock:
        route = respx.post("https://hook.example.com/recv").mock(return_value=Response(200))
        await fire_webhooks([wh], "document.processed", result)
    assert not route.called


@pytest.mark.asyncio
async def test_fire_webhooks_includes_hmac_signature():
    result = _make_result()
    wh = WebhookConfig(url="https://hook.example.com/recv", events=["document.processed"],
                       secret="s3cr3t")
    captured_headers: dict = {}
    captured_body: bytes = b""

    with respx.mock:
        def _capture(request):
            nonlocal captured_headers, captured_body
            captured_headers = dict(request.headers)
            captured_body = request.content
            return Response(200)

        respx.post("https://hook.example.com/recv").mock(side_effect=_capture)
        await fire_webhooks([wh], "document.processed", result)

    assert "x-signature-sha256" in captured_headers
    expected = "sha256=" + hmac.new(b"s3cr3t", captured_body, hashlib.sha256).hexdigest()
    assert captured_headers["x-signature-sha256"] == expected


@pytest.mark.asyncio
async def test_fire_webhooks_payload_structure():
    result = _make_result("success")
    wh = WebhookConfig(url="https://hook.example.com/recv", events=["document.processed"])
    captured_body: bytes = b""

    with respx.mock:
        def _capture(request):
            nonlocal captured_body
            captured_body = request.content
            return Response(200)

        respx.post("https://hook.example.com/recv").mock(side_effect=_capture)
        await fire_webhooks([wh], "document.processed", result)

    payload = json.loads(captured_body)
    assert payload["event"] == "document.processed"
    assert payload["document_id"] == str(result.document_id)
    assert payload["status"] == "success"
    assert "timestamp" in payload
    assert "result" in payload


@pytest.mark.asyncio
async def test_fire_webhooks_tolerates_connection_error():
    result = _make_result()
    wh = WebhookConfig(url="https://unreachable.example.com/hook", events=["document.processed"])
    with respx.mock:
        respx.post("https://unreachable.example.com/hook").mock(
            side_effect=Exception("connection refused")
        )
        # Should not raise
        await fire_webhooks([wh], "document.processed", result)


# ---------------------------------------------------------------------------
# Result statistics endpoint
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_stats_empty_db():
    async with _client() as c:
        r = await c.get("/results/stats")
    assert r.status_code == 200
    body = r.json()
    assert body["total"] == 0
    assert body["avg_duration_ms"] == 0.0
    assert body["recent_24h"] == 0


@pytest.mark.asyncio
async def test_stats_counts_after_uploads():
    async with _client() as c:
        await c.post("/process", files={"file": ("a.txt", _INVOICE, "text/plain")})
        await c.post("/process", files={"file": ("b.txt", _INVOICE, "text/plain")})
        r = await c.get("/results/stats")
    body = r.json()
    assert body["total"] == 2
    assert body["recent_24h"] == 2
    assert isinstance(body["avg_duration_ms"], float)


@pytest.mark.asyncio
async def test_stats_by_status_structure():
    async with _client() as c:
        await c.post("/process", files={"file": ("a.txt", _INVOICE, "text/plain")})
        r = await c.get("/results/stats")
    body = r.json()
    assert "by_status" in body
    assert "by_doc_type" in body
    assert isinstance(body["by_status"], dict)
    assert isinstance(body["by_doc_type"], dict)


# ---------------------------------------------------------------------------
# Delete result endpoint
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_delete_result_removes_it():
    async with _client() as c:
        upload = await c.post("/process", files={"file": ("a.txt", _INVOICE, "text/plain")})
        doc_id = upload.json()["document_id"]
        del_r = await c.delete(f"/results/{doc_id}")
        get_r = await c.get(f"/results/{doc_id}")
    assert del_r.status_code == 204
    assert get_r.status_code == 404


@pytest.mark.asyncio
async def test_delete_missing_result_404():
    async with _client() as c:
        r = await c.delete(f"/results/{uuid4()}")
    assert r.status_code == 404
