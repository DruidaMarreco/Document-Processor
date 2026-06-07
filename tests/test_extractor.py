import json

import pytest

from document_processor.models import Document
from extractor_module import ExtractorModule

ext = ExtractorModule()


def _doc(content: bytes, mimetype: str = "text/plain", filename: str | None = None) -> Document:
    return Document(content=content, mimetype=mimetype, filename=filename)


def _ctx(route: str) -> dict:
    return {"router": {"route": route}}


@pytest.mark.asyncio
async def test_extracts_emails_from_text():
    content = b"Contact us at support@example.com or sales@company.org"
    r = await ext.process(_doc(content), _ctx("text"))
    assert "support@example.com" in r.data["fields"]["emails"]
    assert "sales@company.org" in r.data["fields"]["emails"]


@pytest.mark.asyncio
async def test_extracts_dates_from_text():
    content = b"Invoice date: 2026-01-15. Due date: 2026-02-15."
    r = await ext.process(_doc(content), _ctx("text"))
    assert "2026-01-15" in r.data["fields"]["dates"]
    assert "2026-02-15" in r.data["fields"]["dates"]


@pytest.mark.asyncio
async def test_extracts_amounts_from_text():
    content = b"Total: $1,250.00. Tax: $125.00."
    r = await ext.process(_doc(content), _ctx("text"))
    assert len(r.data["fields"]["amounts"]) >= 1


@pytest.mark.asyncio
async def test_extracts_urls_from_text():
    content = b"Visit https://example.com or https://docs.company.io/api"
    r = await ext.process(_doc(content), _ctx("text"))
    assert "https://example.com" in r.data["fields"]["urls"]


@pytest.mark.asyncio
async def test_parses_json_route():
    payload = {"name": "Alice", "amount": 100}
    r = await ext.process(_doc(json.dumps(payload).encode()), _ctx("json"))
    assert r.data["fields"]["json_data"] == payload
    assert r.data["raw_text"] == ""


@pytest.mark.asyncio
async def test_parses_csv_route():
    content = b"name,age,city\nAlice,30,London\nBob,25,Paris"
    r = await ext.process(_doc(content, "text/csv"), _ctx("spreadsheet"))
    assert "columns" in r.data["fields"]
    assert r.data["fields"]["row_count"] == 2


@pytest.mark.asyncio
async def test_strips_html():
    content = b"<html><body><h1>Hello</h1><p>World</p></body></html>"
    r = await ext.process(_doc(content, "text/html"), _ctx("html"))
    assert "Hello" in r.data["raw_text"]
    assert "World" in r.data["raw_text"]
    assert "<h1>" not in r.data["raw_text"]


@pytest.mark.asyncio
async def test_metadata_always_present():
    r = await ext.process(_doc(b"hi", filename="doc.txt"), _ctx("text"))
    assert r.data["metadata"]["size_bytes"] == 2
    assert r.data["metadata"]["filename"] == "doc.txt"


@pytest.mark.asyncio
async def test_deduplicates_emails():
    content = b"Email: a@b.com, reply to a@b.com, cc a@b.com"
    r = await ext.process(_doc(content), _ctx("text"))
    assert r.data["fields"]["emails"].count("a@b.com") == 1


@pytest.mark.asyncio
async def test_always_success():
    r = await ext.process(_doc(b""), _ctx("generic"))
    assert r.status == "success"
    assert r.module == "extractor"
