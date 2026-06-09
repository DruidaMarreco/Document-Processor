"""Tests for result relations — POST/GET/DELETE /results/{id}/relations."""
from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient

from document_processor import storage
from document_processor.api import app

_INVOICE = b"Invoice No: 1234\nBill To: John\nAmount Due: $500\nDue Date: 2026-01-01"
_EMAIL = b"From: alice@example.com\nTo: bob@example.com\nSubject: Meeting"


@pytest.fixture(autouse=True)
def tmp_db(tmp_path: Path):
    storage.set_db_path(tmp_path / "test.db")
    yield
    storage.set_db_path(Path("data/results.db"))


def _client():
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


async def _upload(client, content: bytes = _INVOICE) -> str:
    r = await client.post("/process", files={"file": ("doc.txt", content, "text/plain")})
    assert r.status_code == 200
    return r.json()["document_id"]


# ---------------------------------------------------------------------------
# Storage unit tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_add_and_get_relation():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL)
    uid1 = __import__("uuid").UUID(id1)
    uid2 = __import__("uuid").UUID(id2)
    entry = await storage.add_relation(uid1, uid2, "related_to")
    assert entry is not None
    assert entry["source_id"] == id1
    assert entry["target_id"] == id2
    assert entry["relation"] == "related_to"
    assert "id" in entry
    assert "created_at" in entry


@pytest.mark.asyncio
async def test_get_relations_returns_both_directions():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL)
    uid1 = __import__("uuid").UUID(id1)
    uid2 = __import__("uuid").UUID(id2)
    await storage.add_relation(uid1, uid2, "related_to")
    # Both source and target should see the relation
    rels1 = await storage.get_relations(uid1)
    rels2 = await storage.get_relations(uid2)
    assert len(rels1) == 1
    assert len(rels2) == 1
    assert rels1[0]["id"] == rels2[0]["id"]


@pytest.mark.asyncio
async def test_duplicate_relation_returns_none():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL)
    uid1 = __import__("uuid").UUID(id1)
    uid2 = __import__("uuid").UUID(id2)
    await storage.add_relation(uid1, uid2, "related_to")
    result = await storage.add_relation(uid1, uid2, "related_to")
    assert result is None


@pytest.mark.asyncio
async def test_different_relation_types_on_same_pair():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL)
    uid1 = __import__("uuid").UUID(id1)
    uid2 = __import__("uuid").UUID(id2)
    r1 = await storage.add_relation(uid1, uid2, "related_to")
    r2 = await storage.add_relation(uid1, uid2, "supersedes")
    assert r1 is not None
    assert r2 is not None
    rels = await storage.get_relations(uid1)
    assert len(rels) == 2


@pytest.mark.asyncio
async def test_delete_relation():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL)
    uid1 = __import__("uuid").UUID(id1)
    uid2 = __import__("uuid").UUID(id2)
    entry = await storage.add_relation(uid1, uid2, "related_to")
    removed = await storage.delete_relation(uid1, entry["id"])
    assert removed is True
    rels = await storage.get_relations(uid1)
    assert len(rels) == 0


@pytest.mark.asyncio
async def test_delete_relation_by_target_also_works():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL)
    uid1 = __import__("uuid").UUID(id1)
    uid2 = __import__("uuid").UUID(id2)
    entry = await storage.add_relation(uid1, uid2, "related_to")
    # target can also delete the relation
    removed = await storage.delete_relation(uid2, entry["id"])
    assert removed is True


@pytest.mark.asyncio
async def test_delete_nonexistent_relation_returns_false():
    async with _client() as c:
        id1 = await _upload(c)
    removed = await storage.delete_relation(__import__("uuid").UUID(id1), 99999)
    assert removed is False


# ---------------------------------------------------------------------------
# API endpoint tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_add_relation_201():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL)
        r = await c.post(f"/results/{id1}/relations",
                         json={"target_id": id2, "relation": "related_to"})
    assert r.status_code == 201


@pytest.mark.asyncio
async def test_add_relation_response_structure():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL)
        r = await c.post(f"/results/{id1}/relations",
                         json={"target_id": id2, "relation": "duplicate_of"})
    body = r.json()
    assert "id" in body
    assert body["source_id"] == id1
    assert body["target_id"] == id2
    assert body["relation"] == "duplicate_of"
    assert "created_at" in body


@pytest.mark.asyncio
async def test_add_relation_all_types():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        for relation in ("related_to", "duplicate_of", "supersedes", "attachment_of"):
            id2 = await _upload(c, _EMAIL)
            r = await c.post(f"/results/{id1}/relations",
                             json={"target_id": id2, "relation": relation})
            assert r.status_code == 201, f"relation={relation} failed"


@pytest.mark.asyncio
async def test_add_relation_invalid_type_400():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL)
        r = await c.post(f"/results/{id1}/relations",
                         json={"target_id": id2, "relation": "invented_type"})
    assert r.status_code == 400


@pytest.mark.asyncio
async def test_add_relation_404_source_missing():
    async with _client() as c:
        id2 = await _upload(c)
        r = await c.post(f"/results/{uuid4()}/relations",
                         json={"target_id": id2, "relation": "related_to"})
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_add_relation_404_target_missing():
    async with _client() as c:
        id1 = await _upload(c)
        r = await c.post(f"/results/{id1}/relations",
                         json={"target_id": str(uuid4()), "relation": "related_to"})
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_add_duplicate_relation_409():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL)
        await c.post(f"/results/{id1}/relations",
                     json={"target_id": id2, "relation": "related_to"})
        r = await c.post(f"/results/{id1}/relations",
                         json={"target_id": id2, "relation": "related_to"})
    assert r.status_code == 409


@pytest.mark.asyncio
async def test_list_relations_empty():
    async with _client() as c:
        id1 = await _upload(c)
        r = await c.get(f"/results/{id1}/relations")
    assert r.status_code == 200
    body = r.json()
    assert body["document_id"] == id1
    assert body["relations"] == []


@pytest.mark.asyncio
async def test_list_relations_shows_added():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL)
        await c.post(f"/results/{id1}/relations",
                     json={"target_id": id2, "relation": "supersedes"})
        r = await c.get(f"/results/{id1}/relations")
    assert len(r.json()["relations"]) == 1
    assert r.json()["relations"][0]["relation"] == "supersedes"


@pytest.mark.asyncio
async def test_list_relations_404_for_missing():
    async with _client() as c:
        r = await c.get(f"/results/{uuid4()}/relations")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_delete_relation_204():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL)
        r_add = await c.post(f"/results/{id1}/relations",
                             json={"target_id": id2, "relation": "related_to"})
        rel_id = r_add.json()["id"]
        r_del = await c.delete(f"/results/{id1}/relations/{rel_id}")
    assert r_del.status_code == 204


@pytest.mark.asyncio
async def test_delete_relation_removes_it():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL)
        r_add = await c.post(f"/results/{id1}/relations",
                             json={"target_id": id2, "relation": "related_to"})
        rel_id = r_add.json()["id"]
        await c.delete(f"/results/{id1}/relations/{rel_id}")
        r_list = await c.get(f"/results/{id1}/relations")
    assert r_list.json()["relations"] == []


@pytest.mark.asyncio
async def test_delete_relation_404_missing():
    async with _client() as c:
        id1 = await _upload(c)
        r = await c.delete(f"/results/{id1}/relations/99999")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_relation_in_audit_log():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL)
        await c.post(f"/results/{id1}/relations",
                     json={"target_id": id2, "relation": "related_to"})
        r = await c.get(f"/results/{id1}/audit")
    actions = [e["action"] for e in r.json()["entries"]]
    assert "relation_added" in actions
