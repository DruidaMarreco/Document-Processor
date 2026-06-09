"""Tests for tag statistics and autocomplete — GET /results/stats/tags and GET /results/tags."""
from __future__ import annotations

from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

from document_processor import storage
from document_processor.api import app

_INVOICE = b"Invoice No: 1234\nBill To: John\nAmount Due: $500\nDue Date: 2026-01-01"
_EMAIL = b"From: alice@example.com\nTo: bob@example.com\nSubject: Meeting"
_CONTRACT = b"This agreement is entered between Party A and Party B on 2026-01-01."


@pytest.fixture(autouse=True)
def tmp_db(tmp_path: Path):
    storage.set_db_path(tmp_path / "test.db")
    yield
    storage.set_db_path(Path("data/results.db"))


def _client():
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


async def _upload(client, content: bytes = _INVOICE, filename: str = "doc.txt") -> str:
    r = await client.post("/process", files={"file": (filename, content, "text/plain")})
    assert r.status_code == 200
    return r.json()["document_id"]


async def _tag(client, doc_id: str, tags: list[str]) -> None:
    await client.post(f"/results/{doc_id}/tags", json={"tags": tags})


# ---------------------------------------------------------------------------
# Storage unit tests — get_tag_stats
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_tag_stats_empty_db():
    result = await storage.get_tag_stats()
    assert result == []


@pytest.mark.asyncio
async def test_tag_stats_single_tag():
    async with _client() as c:
        doc_id = await _upload(c)
        await _tag(c, doc_id, ["invoice"])
    stats = await storage.get_tag_stats()
    assert len(stats) == 1
    assert stats[0]["tag"] == "invoice"
    assert stats[0]["count"] == 1


@pytest.mark.asyncio
async def test_tag_stats_multiple_tags_one_doc():
    async with _client() as c:
        doc_id = await _upload(c)
        await _tag(c, doc_id, ["invoice", "urgent", "2026"])
    stats = await storage.get_tag_stats()
    tag_names = {s["tag"] for s in stats}
    assert {"invoice", "urgent", "2026"} == tag_names
    for s in stats:
        assert s["count"] == 1


@pytest.mark.asyncio
async def test_tag_stats_count_reflects_multiple_docs():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL, "email.txt")
        await _tag(c, id1, ["shared"])
        await _tag(c, id2, ["shared"])
    stats = await storage.get_tag_stats()
    shared = next(s for s in stats if s["tag"] == "shared")
    assert shared["count"] == 2


@pytest.mark.asyncio
async def test_tag_stats_sorted_by_count_desc():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL, "email.txt")
        id3 = await _upload(c, _CONTRACT, "contract.txt")
        await _tag(c, id1, ["popular"])
        await _tag(c, id2, ["popular"])
        await _tag(c, id3, ["popular"])
        await _tag(c, id1, ["rare"])
    stats = await storage.get_tag_stats()
    assert stats[0]["tag"] == "popular"
    assert stats[0]["count"] == 3
    assert stats[-1]["tag"] == "rare"
    assert stats[-1]["count"] == 1


@pytest.mark.asyncio
async def test_tag_stats_unique_per_doc():
    """Each (doc, tag) pair is unique — adding same tag twice to one doc counts as 1."""
    async with _client() as c:
        doc_id = await _upload(c)
        await _tag(c, doc_id, ["dup"])
        await _tag(c, doc_id, ["dup"])
    stats = await storage.get_tag_stats()
    dup = next(s for s in stats if s["tag"] == "dup")
    assert dup["count"] == 1


# ---------------------------------------------------------------------------
# Storage unit tests — get_all_tags
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_all_tags_empty_db():
    result = await storage.get_all_tags()
    assert result == []


@pytest.mark.asyncio
async def test_all_tags_returns_all():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL, "email.txt")
        await _tag(c, id1, ["invoice", "finance"])
        await _tag(c, id2, ["email", "finance"])
    tags = await storage.get_all_tags()
    assert set(tags) == {"invoice", "finance", "email"}


@pytest.mark.asyncio
async def test_all_tags_no_duplicates():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL, "email.txt")
        await _tag(c, id1, ["shared"])
        await _tag(c, id2, ["shared"])
    tags = await storage.get_all_tags()
    assert tags.count("shared") == 1


@pytest.mark.asyncio
async def test_all_tags_prefix_filter():
    async with _client() as c:
        doc_id = await _upload(c)
        await _tag(c, doc_id, ["invoice", "important", "email", "urgent"])
    tags = await storage.get_all_tags(prefix="inv")
    assert "invoice" in tags
    assert "important" not in tags
    assert "email" not in tags


@pytest.mark.asyncio
async def test_all_tags_prefix_case_insensitive():
    async with _client() as c:
        doc_id = await _upload(c)
        await _tag(c, doc_id, ["invoice"])
    tags = await storage.get_all_tags(prefix="INV")
    assert "invoice" in tags


@pytest.mark.asyncio
async def test_all_tags_sorted_alphabetically():
    async with _client() as c:
        doc_id = await _upload(c)
        await _tag(c, doc_id, ["zebra", "apple", "mango"])
    tags = await storage.get_all_tags()
    assert tags == sorted(tags)


@pytest.mark.asyncio
async def test_all_tags_limit():
    async with _client() as c:
        doc_id = await _upload(c)
        await _tag(c, doc_id, [f"tag{i:02d}" for i in range(10)])
    tags = await storage.get_all_tags(limit=3)
    assert len(tags) == 3


# ---------------------------------------------------------------------------
# API endpoint tests — /results/stats/tags
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_stats_tags_endpoint_200():
    async with _client() as c:
        r = await c.get("/results/stats/tags")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_stats_tags_response_structure():
    async with _client() as c:
        r = await c.get("/results/stats/tags")
    body = r.json()
    assert "tags" in body
    assert isinstance(body["tags"], list)


@pytest.mark.asyncio
async def test_stats_tags_empty_when_no_tags():
    async with _client() as c:
        await _upload(c)
        r = await c.get("/results/stats/tags")
    assert r.json()["tags"] == []


@pytest.mark.asyncio
async def test_stats_tags_populated_after_tagging():
    async with _client() as c:
        doc_id = await _upload(c)
        await _tag(c, doc_id, ["invoice"])
        r = await c.get("/results/stats/tags")
    tags = r.json()["tags"]
    assert len(tags) == 1
    assert tags[0]["tag"] == "invoice"
    assert tags[0]["count"] == 1


@pytest.mark.asyncio
async def test_stats_tags_count_matches_docs():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL, "email.txt")
        await _tag(c, id1, ["common"])
        await _tag(c, id2, ["common"])
        r = await c.get("/results/stats/tags")
    tags = r.json()["tags"]
    common = next(t for t in tags if t["tag"] == "common")
    assert common["count"] == 2


@pytest.mark.asyncio
async def test_stats_tags_sorted_by_count_desc():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL, "email.txt")
        await _tag(c, id1, ["top"])
        await _tag(c, id2, ["top"])
        await _tag(c, id1, ["bottom"])
        r = await c.get("/results/stats/tags")
    tags = r.json()["tags"]
    counts = [t["count"] for t in tags]
    assert counts == sorted(counts, reverse=True)


# ---------------------------------------------------------------------------
# API endpoint tests — /results/tags (autocomplete)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_tags_autocomplete_endpoint_200():
    async with _client() as c:
        r = await c.get("/results/tags")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_tags_autocomplete_response_structure():
    async with _client() as c:
        r = await c.get("/results/tags")
    body = r.json()
    assert "tags" in body
    assert isinstance(body["tags"], list)


@pytest.mark.asyncio
async def test_tags_autocomplete_empty_when_none():
    async with _client() as c:
        r = await c.get("/results/tags")
    assert r.json()["tags"] == []


@pytest.mark.asyncio
async def test_tags_autocomplete_returns_all_without_q():
    async with _client() as c:
        doc_id = await _upload(c)
        await _tag(c, doc_id, ["alpha", "beta", "gamma"])
        r = await c.get("/results/tags")
    assert set(r.json()["tags"]) == {"alpha", "beta", "gamma"}


@pytest.mark.asyncio
async def test_tags_autocomplete_filters_with_q():
    async with _client() as c:
        doc_id = await _upload(c)
        await _tag(c, doc_id, ["invoice", "important", "email"])
        r = await c.get("/results/tags?q=inv")
    tags = r.json()["tags"]
    assert "invoice" in tags
    assert "important" not in tags
    assert "email" not in tags


@pytest.mark.asyncio
async def test_tags_autocomplete_no_duplicates():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL, "email.txt")
        await _tag(c, id1, ["shared"])
        await _tag(c, id2, ["shared"])
        r = await c.get("/results/tags")
    assert r.json()["tags"].count("shared") == 1


@pytest.mark.asyncio
async def test_tags_autocomplete_limit_param():
    async with _client() as c:
        doc_id = await _upload(c)
        await _tag(c, doc_id, [f"tag{i:02d}" for i in range(10)])
        r = await c.get("/results/tags?limit=3")
    assert len(r.json()["tags"]) == 3


@pytest.mark.asyncio
async def test_tags_autocomplete_empty_when_no_match():
    async with _client() as c:
        doc_id = await _upload(c)
        await _tag(c, doc_id, ["invoice"])
        r = await c.get("/results/tags?q=zzz")
    assert r.json()["tags"] == []
