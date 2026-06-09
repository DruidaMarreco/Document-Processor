"""Tests for API key quotas — GET/PUT/DELETE /api-keys/{prefix}/quota."""
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


async def _create_key(client, name: str = "test-key") -> tuple[str, str]:
    """Create an API key. Returns (prefix, full_key)."""
    r = await client.post("/api-keys", json={"name": name})
    assert r.status_code == 201
    full_key = r.json()["key"]
    return full_key[:8], full_key


async def _upload(client, api_key: str | None = None) -> str:
    headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
    r = await client.post(
        "/process",
        files={"file": ("doc.txt", _INVOICE, "text/plain")},
        headers=headers,
    )
    assert r.status_code == 200
    return r.json()["document_id"]


# ---------------------------------------------------------------------------
# Storage unit tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_quota_returns_none_for_missing_prefix():
    result = await storage.get_api_key_quota("nonexistent")
    assert result is None


@pytest.mark.asyncio
async def test_get_quota_no_limit_set():
    async with _client() as c:
        prefix, _ = await _create_key(c)
    quota = await storage.get_api_key_quota(prefix)
    assert quota is not None
    assert quota["daily_limit"] is None
    assert quota["remaining"] is None


@pytest.mark.asyncio
async def test_get_quota_has_required_fields():
    async with _client() as c:
        prefix, _ = await _create_key(c)
    quota = await storage.get_api_key_quota(prefix)
    for field in ("prefix", "daily_limit", "used_today", "remaining"):
        assert field in quota


@pytest.mark.asyncio
async def test_get_quota_prefix_matches():
    async with _client() as c:
        prefix, _ = await _create_key(c)
    quota = await storage.get_api_key_quota(prefix)
    assert quota["prefix"] == prefix


@pytest.mark.asyncio
async def test_set_quota_persists():
    async with _client() as c:
        prefix, _ = await _create_key(c)
    await storage.set_api_key_quota(prefix, 100)
    quota = await storage.get_api_key_quota(prefix)
    assert quota["daily_limit"] == 100


@pytest.mark.asyncio
async def test_set_quota_updates_existing():
    async with _client() as c:
        prefix, _ = await _create_key(c)
    await storage.set_api_key_quota(prefix, 50)
    await storage.set_api_key_quota(prefix, 200)
    quota = await storage.get_api_key_quota(prefix)
    assert quota["daily_limit"] == 200


@pytest.mark.asyncio
async def test_remaining_calculated_correctly():
    async with _client() as c:
        prefix, key = await _create_key(c)
        await _upload(c, key)
        await _upload(c, key)
    await storage.set_api_key_quota(prefix, 10)
    quota = await storage.get_api_key_quota(prefix)
    assert quota["used_today"] == 2
    assert quota["remaining"] == 8


@pytest.mark.asyncio
async def test_remaining_capped_at_zero():
    async with _client() as c:
        prefix, key = await _create_key(c)
        await _upload(c, key)
        await _upload(c, key)
        await _upload(c, key)
    await storage.set_api_key_quota(prefix, 1)
    quota = await storage.get_api_key_quota(prefix)
    assert quota["remaining"] == 0


@pytest.mark.asyncio
async def test_delete_quota_returns_true():
    async with _client() as c:
        prefix, _ = await _create_key(c)
    await storage.set_api_key_quota(prefix, 100)
    deleted = await storage.delete_api_key_quota(prefix)
    assert deleted is True


@pytest.mark.asyncio
async def test_delete_quota_removes_limit():
    async with _client() as c:
        prefix, _ = await _create_key(c)
    await storage.set_api_key_quota(prefix, 100)
    await storage.delete_api_key_quota(prefix)
    quota = await storage.get_api_key_quota(prefix)
    assert quota["daily_limit"] is None


@pytest.mark.asyncio
async def test_delete_quota_returns_false_when_no_limit():
    async with _client() as c:
        prefix, _ = await _create_key(c)
    deleted = await storage.delete_api_key_quota(prefix)
    assert deleted is False


# ---------------------------------------------------------------------------
# API endpoint tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_quota_endpoint_200():
    async with _client() as c:
        prefix, _ = await _create_key(c)
        r = await c.get(f"/api-keys/{prefix}/quota")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_get_quota_endpoint_404_missing():
    async with _client() as c:
        r = await c.get("/api-keys/nonexistent/quota")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_get_quota_response_structure():
    async with _client() as c:
        prefix, _ = await _create_key(c)
        r = await c.get(f"/api-keys/{prefix}/quota")
    body = r.json()
    for field in ("prefix", "daily_limit", "used_today", "remaining"):
        assert field in body


@pytest.mark.asyncio
async def test_get_quota_no_limit_initially():
    async with _client() as c:
        prefix, _ = await _create_key(c)
        r = await c.get(f"/api-keys/{prefix}/quota")
    body = r.json()
    assert body["daily_limit"] is None
    assert body["remaining"] is None


@pytest.mark.asyncio
async def test_put_quota_endpoint_204():
    async with _client() as c:
        prefix, _ = await _create_key(c)
        r = await c.put(f"/api-keys/{prefix}/quota", json={"daily_limit": 50})
    assert r.status_code == 204


@pytest.mark.asyncio
async def test_put_quota_404_missing():
    async with _client() as c:
        r = await c.put("/api-keys/nonexistent/quota", json={"daily_limit": 50})
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_put_quota_invalid_limit_422():
    async with _client() as c:
        prefix, _ = await _create_key(c)
        r = await c.put(f"/api-keys/{prefix}/quota", json={"daily_limit": 0})
    assert r.status_code == 422


@pytest.mark.asyncio
async def test_put_quota_persists_via_get():
    async with _client() as c:
        prefix, _ = await _create_key(c)
        await c.put(f"/api-keys/{prefix}/quota", json={"daily_limit": 75})
        r = await c.get(f"/api-keys/{prefix}/quota")
    assert r.json()["daily_limit"] == 75


@pytest.mark.asyncio
async def test_delete_quota_endpoint_204():
    async with _client() as c:
        prefix, _ = await _create_key(c)
        await c.put(f"/api-keys/{prefix}/quota", json={"daily_limit": 100})
        r = await c.delete(f"/api-keys/{prefix}/quota")
    assert r.status_code == 204


@pytest.mark.asyncio
async def test_delete_quota_404_missing_prefix():
    async with _client() as c:
        r = await c.delete("/api-keys/nonexistent/quota")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_delete_quota_clears_limit():
    async with _client() as c:
        prefix, _ = await _create_key(c)
        await c.put(f"/api-keys/{prefix}/quota", json={"daily_limit": 100})
        await c.delete(f"/api-keys/{prefix}/quota")
        r = await c.get(f"/api-keys/{prefix}/quota")
    assert r.json()["daily_limit"] is None


@pytest.mark.asyncio
async def test_used_today_reflects_uploads():
    async with _client() as c:
        prefix, key = await _create_key(c)
        await _upload(c, key)
        await _upload(c, key)
        r = await c.get(f"/api-keys/{prefix}/quota")
    assert r.json()["used_today"] == 2
