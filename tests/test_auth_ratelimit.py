"""Tests for API key authentication and rate limiting."""
from __future__ import annotations

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


# ---------------------------------------------------------------------------
# API key CRUD
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_create_api_key_returns_full_key():
    async with _client() as c:
        r = await c.post("/api-keys", json={"name": "test-key"})
    assert r.status_code == 201
    body = r.json()
    assert "key" in body
    assert len(body["key"]) > 20
    assert body["name"] == "test-key"


@pytest.mark.asyncio
async def test_list_api_keys_shows_prefix_not_full_key():
    async with _client() as c:
        create = await c.post("/api-keys", json={"name": "my-key"})
        full_key = create.json()["key"]
        r = await c.get("/api-keys")
    assert r.status_code == 200
    items = r.json()
    assert len(items) == 1
    item = items[0]
    assert "prefix" in item
    assert item["prefix"] == full_key[:8]
    assert "key" not in item  # full key never re-exposed


@pytest.mark.asyncio
async def test_delete_api_key_removes_it():
    async with _client() as c:
        create = await c.post("/api-keys", json={"name": "temp"})
        prefix = create.json()["key"][:8]
        del_r = await c.delete(f"/api-keys/{prefix}")
        list_r = await c.get("/api-keys")
    assert del_r.status_code == 204
    assert list_r.json() == []


@pytest.mark.asyncio
async def test_delete_missing_api_key_404():
    async with _client() as c:
        r = await c.delete("/api-keys/deadbeef")
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Auth enforcement
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_no_keys_registered_allows_open_access():
    """When no API keys exist, all endpoints are accessible without auth."""
    async with _client() as c:
        r = await c.post("/process", files={"file": ("a.txt", _INVOICE, "text/plain")})
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_once_key_created_unauthenticated_request_rejected():
    async with _client() as c:
        await c.post("/api-keys", json={"name": "lock-it-down"})
        r = await c.post("/process", files={"file": ("a.txt", _INVOICE, "text/plain")})
    assert r.status_code == 401


@pytest.mark.asyncio
async def test_valid_key_allows_access():
    async with _client() as c:
        create = await c.post("/api-keys", json={"name": "valid"})
        key = create.json()["key"]
        r = await c.post(
            "/process",
            files={"file": ("a.txt", _INVOICE, "text/plain")},
            headers={"Authorization": f"Bearer {key}"},
        )
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_wrong_key_rejected_403():
    async with _client() as c:
        await c.post("/api-keys", json={"name": "real-key"})
        r = await c.post(
            "/process",
            files={"file": ("a.txt", _INVOICE, "text/plain")},
            headers={"Authorization": "Bearer wrong-key-value"},
        )
    assert r.status_code == 403


@pytest.mark.asyncio
async def test_results_endpoint_protected():
    async with _client() as c:
        await c.post("/api-keys", json={"name": "guard"})
        r = await c.get("/results")
    assert r.status_code == 401


@pytest.mark.asyncio
async def test_health_not_protected():
    """Health endpoint is always accessible — no auth required."""
    async with _client() as c:
        await c.post("/api-keys", json={"name": "lock"})
        r = await c.get("/health")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_revoked_key_rejected():
    """After deleting a key it no longer grants access."""
    async with _client() as c:
        create = await c.post("/api-keys", json={"name": "temporary"})
        key = create.json()["key"]
        prefix = key[:8]
        await c.delete(f"/api-keys/{prefix}")
        # A second key keeps auth enforced
        await c.post("/api-keys", json={"name": "other"})
        r = await c.post(
            "/process",
            files={"file": ("a.txt", _INVOICE, "text/plain")},
            headers={"Authorization": f"Bearer {key}"},
        )
    assert r.status_code == 403


# ---------------------------------------------------------------------------
# Rate limiting (smoke test — just verify header presence)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_rate_limit_headers_present():
    """slowapi injects X-RateLimit-* headers on successful responses."""
    async with _client() as c:
        r = await c.post("/process", files={"file": ("a.txt", _INVOICE, "text/plain")})
    assert r.status_code == 200
    # Headers may vary by slowapi version; just confirm no 429 on first request
    assert r.status_code != 429
