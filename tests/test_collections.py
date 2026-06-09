"""Tests for document collections (POST/GET/DELETE /collections, members)."""
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


async def _upload(client) -> str:
    r = await client.post("/process", files={"file": ("a.txt", _INVOICE, "text/plain")})
    assert r.status_code == 200
    return r.json()["document_id"]


async def _create_col(client, name="Q1 Invoices", desc=None) -> str:
    body = {"name": name}
    if desc:
        body["description"] = desc
    r = await client.post("/collections", json=body)
    assert r.status_code == 201
    return r.json()["id"]


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_create_collection():
    async with _client() as c:
        r = await c.post("/collections", json={"name": "My Docs", "description": "test"})
    assert r.status_code == 201
    body = r.json()
    assert body["name"] == "My Docs"
    assert body["description"] == "test"
    assert "id" in body
    assert "created_at" in body


@pytest.mark.asyncio
async def test_create_collection_no_description():
    async with _client() as c:
        r = await c.post("/collections", json={"name": "Bare"})
    assert r.status_code == 201
    assert r.json()["description"] is None


@pytest.mark.asyncio
async def test_get_collection():
    async with _client() as c:
        col_id = await _create_col(c, "Archive")
        r = await c.get(f"/collections/{col_id}")
    assert r.status_code == 200
    assert r.json()["name"] == "Archive"


@pytest.mark.asyncio
async def test_get_missing_collection_404():
    async with _client() as c:
        r = await c.get(f"/collections/{uuid4()}")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_list_collections():
    async with _client() as c:
        await _create_col(c, "A")
        await _create_col(c, "B")
        r = await c.get("/collections")
    names = [col["name"] for col in r.json()]
    assert "A" in names
    assert "B" in names


@pytest.mark.asyncio
async def test_delete_collection():
    async with _client() as c:
        col_id = await _create_col(c)
        r_del = await c.delete(f"/collections/{col_id}")
        r_get = await c.get(f"/collections/{col_id}")
    assert r_del.status_code == 204
    assert r_get.status_code == 404


@pytest.mark.asyncio
async def test_delete_missing_collection_404():
    async with _client() as c:
        r = await c.delete(f"/collections/{uuid4()}")
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Members
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_add_and_list_members():
    async with _client() as c:
        col_id = await _create_col(c)
        doc_id = await _upload(c)
        await c.post(f"/collections/{col_id}/members", json={"ids": [doc_id]})
        r = await c.get(f"/collections/{col_id}/members")
    assert r.status_code == 200
    body = r.json()
    assert body["total"] == 1
    assert body["results"][0]["document_id"] == doc_id


@pytest.mark.asyncio
async def test_add_multiple_members():
    async with _client() as c:
        col_id = await _create_col(c)
        id1 = await _upload(c)
        id2 = await _upload(c)
        await c.post(f"/collections/{col_id}/members", json={"ids": [id1, id2]})
        r = await c.get(f"/collections/{col_id}/members")
    assert r.json()["total"] == 2


@pytest.mark.asyncio
async def test_add_member_idempotent():
    async with _client() as c:
        col_id = await _create_col(c)
        doc_id = await _upload(c)
        await c.post(f"/collections/{col_id}/members", json={"ids": [doc_id]})
        await c.post(f"/collections/{col_id}/members", json={"ids": [doc_id]})
        r = await c.get(f"/collections/{col_id}/members")
    assert r.json()["total"] == 1


@pytest.mark.asyncio
async def test_remove_member():
    async with _client() as c:
        col_id = await _create_col(c)
        doc_id = await _upload(c)
        await c.post(f"/collections/{col_id}/members", json={"ids": [doc_id]})
        r_del = await c.delete(f"/collections/{col_id}/members/{doc_id}")
        r_list = await c.get(f"/collections/{col_id}/members")
    assert r_del.status_code == 204
    assert r_list.json()["total"] == 0


@pytest.mark.asyncio
async def test_remove_non_member_404():
    async with _client() as c:
        col_id = await _create_col(c)
        r = await c.delete(f"/collections/{col_id}/members/{uuid4()}")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_add_to_missing_collection_404():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.post(f"/collections/{uuid4()}/members", json={"ids": [doc_id]})
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_list_members_missing_collection_404():
    async with _client() as c:
        r = await c.get(f"/collections/{uuid4()}/members")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_delete_collection_removes_members():
    """Deleting a collection also removes its membership records."""
    async with _client() as c:
        col_id = await _create_col(c)
        doc_id = await _upload(c)
        await c.post(f"/collections/{col_id}/members", json={"ids": [doc_id]})
        await c.delete(f"/collections/{col_id}")
    # Confirm the collection is gone
    col = await storage.get_collection(col_id)
    assert col is None


@pytest.mark.asyncio
async def test_members_pagination():
    async with _client() as c:
        col_id = await _create_col(c)
        ids = [await _upload(c) for _ in range(4)]
        await c.post(f"/collections/{col_id}/members", json={"ids": ids})
        r1 = await c.get(f"/collections/{col_id}/members?limit=2&offset=0")
        r2 = await c.get(f"/collections/{col_id}/members?limit=2&offset=2")
    assert r1.json()["total"] == 4
    page1_ids = {res["document_id"] for res in r1.json()["results"]}
    page2_ids = {res["document_id"] for res in r2.json()["results"]}
    assert len(page1_ids) == 2
    assert page1_ids.isdisjoint(page2_ids)


@pytest.mark.asyncio
async def test_add_empty_ids_rejected():
    async with _client() as c:
        col_id = await _create_col(c)
        r = await c.post(f"/collections/{col_id}/members", json={"ids": []})
    assert r.status_code == 400


@pytest.mark.asyncio
async def test_add_over_limit_rejected():
    async with _client() as c:
        col_id = await _create_col(c)
        r = await c.post(
            f"/collections/{col_id}/members",
            json={"ids": [str(uuid4()) for _ in range(101)]},
        )
    assert r.status_code == 400
