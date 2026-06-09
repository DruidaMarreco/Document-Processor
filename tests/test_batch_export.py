"""Tests for POST /results/batch-export — ZIP archive of multiple results."""
from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient

from document_processor import storage
from document_processor.api import app

_INVOICE = b"Invoice No: 1234\nBill To: John\nAmount Due: $500\nDue Date: 2026-01-01"
_EMAIL = b"From: alice@example.com\nTo: bob@example.com\nSubject: Meeting notes"


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


def _open_zip(content: bytes) -> zipfile.ZipFile:
    return zipfile.ZipFile(io.BytesIO(content))


# ---------------------------------------------------------------------------
# Storage unit tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_batch_export_zip_returns_bytes():
    async with _client() as c:
        doc_id = await _upload(c)
    zip_bytes = await storage.batch_export_zip([__import__("uuid").UUID(doc_id)], format="json")
    assert isinstance(zip_bytes, bytes)
    assert len(zip_bytes) > 0


@pytest.mark.asyncio
async def test_batch_export_zip_contains_one_file_per_result():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL, "email.txt")
    zip_bytes = await storage.batch_export_zip(
        [__import__("uuid").UUID(id1), __import__("uuid").UUID(id2)], format="json"
    )
    with _open_zip(zip_bytes) as zf:
        names = zf.namelist()
    assert len(names) == 2
    assert f"{id1}.json" in names
    assert f"{id2}.json" in names


@pytest.mark.asyncio
async def test_batch_export_zip_skips_missing_ids():
    async with _client() as c:
        doc_id = await _upload(c)
    missing = uuid4()
    zip_bytes = await storage.batch_export_zip(
        [__import__("uuid").UUID(doc_id), missing], format="json"
    )
    with _open_zip(zip_bytes) as zf:
        names = zf.namelist()
    assert len(names) == 1
    assert f"{doc_id}.json" in names


@pytest.mark.asyncio
async def test_batch_export_all_missing_returns_empty_zip():
    zip_bytes = await storage.batch_export_zip([uuid4(), uuid4()], format="json")
    with _open_zip(zip_bytes) as zf:
        assert zf.namelist() == []


@pytest.mark.asyncio
async def test_batch_export_json_content_is_valid():
    async with _client() as c:
        doc_id = await _upload(c)
    zip_bytes = await storage.batch_export_zip(
        [__import__("uuid").UUID(doc_id)], format="json"
    )
    with _open_zip(zip_bytes) as zf:
        data = json.loads(zf.read(f"{doc_id}.json"))
    assert data["document_id"] == doc_id


@pytest.mark.asyncio
async def test_batch_export_csv_extension():
    async with _client() as c:
        doc_id = await _upload(c)
    zip_bytes = await storage.batch_export_zip(
        [__import__("uuid").UUID(doc_id)], format="csv"
    )
    with _open_zip(zip_bytes) as zf:
        assert f"{doc_id}.csv" in zf.namelist()


@pytest.mark.asyncio
async def test_batch_export_markdown_extension():
    async with _client() as c:
        doc_id = await _upload(c)
    zip_bytes = await storage.batch_export_zip(
        [__import__("uuid").UUID(doc_id)], format="markdown"
    )
    with _open_zip(zip_bytes) as zf:
        assert f"{doc_id}.md" in zf.namelist()


@pytest.mark.asyncio
async def test_batch_export_xml_extension():
    async with _client() as c:
        doc_id = await _upload(c)
    zip_bytes = await storage.batch_export_zip(
        [__import__("uuid").UUID(doc_id)], format="xml"
    )
    with _open_zip(zip_bytes) as zf:
        assert f"{doc_id}.xml" in zf.namelist()


# ---------------------------------------------------------------------------
# API endpoint tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_batch_export_endpoint_200():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.post("/results/batch-export", json={"ids": [doc_id], "format": "json"})
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_batch_export_content_type_zip():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.post("/results/batch-export", json={"ids": [doc_id], "format": "json"})
    assert r.headers["content-type"] == "application/zip"


@pytest.mark.asyncio
async def test_batch_export_content_disposition():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.post("/results/batch-export", json={"ids": [doc_id], "format": "json"})
    assert "export.zip" in r.headers["content-disposition"]


@pytest.mark.asyncio
async def test_batch_export_zip_is_valid():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.post("/results/batch-export", json={"ids": [doc_id], "format": "json"})
    with _open_zip(r.content) as zf:
        assert f"{doc_id}.json" in zf.namelist()


@pytest.mark.asyncio
async def test_batch_export_empty_ids_400():
    async with _client() as c:
        r = await c.post("/results/batch-export", json={"ids": [], "format": "json"})
    assert r.status_code == 400


@pytest.mark.asyncio
async def test_batch_export_invalid_format_400():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.post("/results/batch-export", json={"ids": [doc_id], "format": "pdf"})
    assert r.status_code == 400


@pytest.mark.asyncio
async def test_batch_export_over_100_ids_400():
    ids = [str(uuid4()) for _ in range(101)]
    async with _client() as c:
        r = await c.post("/results/batch-export", json={"ids": ids, "format": "json"})
    assert r.status_code == 400


@pytest.mark.asyncio
async def test_batch_export_multiple_formats():
    async with _client() as c:
        id1 = await _upload(c, _INVOICE)
        id2 = await _upload(c, _EMAIL, "email.txt")
        for fmt in ("json", "csv", "markdown", "xml"):
            r = await c.post("/results/batch-export",
                             json={"ids": [id1, id2], "format": fmt})
            assert r.status_code == 200, f"format={fmt} failed"
            with _open_zip(r.content) as zf:
                assert len(zf.namelist()) == 2


@pytest.mark.asyncio
async def test_batch_export_default_format_is_json():
    async with _client() as c:
        doc_id = await _upload(c)
        r = await c.post("/results/batch-export", json={"ids": [doc_id]})
    assert r.status_code == 200
    with _open_zip(r.content) as zf:
        assert f"{doc_id}.json" in zf.namelist()
