"""Tests for GET /results/{id}/export — markdown and xml format extensions."""
from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient

from document_processor import storage
from document_processor.api import app
from document_processor.models import PipelineResult, StageResult
from document_processor.storage import result_to_markdown, result_to_xml

_INVOICE = b"Invoice No: 1234\nBill To: John\nAmount Due: $500\nDue Date: 2026-01-01"


@pytest.fixture(autouse=True)
def tmp_db(tmp_path: Path):
    storage.set_db_path(tmp_path / "test.db")
    yield
    storage.set_db_path(Path("data/results.db"))


def _client():
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


def _make_result(doc_type: str = "invoice") -> PipelineResult:
    return PipelineResult(
        document_id=uuid4(),
        status="success",
        total_duration_ms=42.0,
        stages=[
            StageResult(
                module="classifier", status="success",
                data={"type": doc_type, "confidence": 0.95, "method": "keyword"},
                duration_ms=5.0,
            ),
            StageResult(
                module="extractor", status="success",
                data={
                    "metadata": {"filename": "inv.txt"},
                    "fields": {
                        "invoice_numbers": ["1234"],
                        "amounts": ["$500"],
                    },
                },
                duration_ms=10.0,
            ),
            StageResult(
                module="validator", status="success",
                data={"valid": True, "violations": [], "warnings": []},
                duration_ms=2.0,
            ),
        ],
    )


# ---------------------------------------------------------------------------
# Unit tests for helpers
# ---------------------------------------------------------------------------

def test_result_to_markdown_contains_document_id():
    result = _make_result()
    md = result_to_markdown(result)
    assert str(result.document_id) in md


def test_result_to_markdown_contains_doc_type():
    result = _make_result("invoice")
    md = result_to_markdown(result)
    assert "invoice" in md


def test_result_to_markdown_contains_fields():
    result = _make_result()
    md = result_to_markdown(result)
    assert "invoice_numbers" in md
    assert "1234" in md


def test_result_to_markdown_contains_stages():
    result = _make_result()
    md = result_to_markdown(result)
    assert "classifier" in md
    assert "extractor" in md
    assert "validator" in md


def test_result_to_markdown_has_table_headers():
    result = _make_result()
    md = result_to_markdown(result)
    assert "| Field | Value |" in md


def test_result_to_xml_is_valid_structure():
    result = _make_result()
    xml = result_to_xml(result)
    assert xml.startswith('<?xml version="1.0"')
    assert "<result>" in xml
    assert "</result>" in xml


def test_result_to_xml_contains_document_id():
    result = _make_result()
    xml = result_to_xml(result)
    assert str(result.document_id) in xml


def test_result_to_xml_contains_classification():
    result = _make_result("invoice")
    xml = result_to_xml(result)
    assert "<classification>" in xml
    assert "invoice" in xml
    assert "<confidence>" in xml


def test_result_to_xml_contains_fields():
    result = _make_result()
    xml = result_to_xml(result)
    assert "<fields>" in xml
    assert "1234" in xml


def test_result_to_xml_contains_stages():
    result = _make_result()
    xml = result_to_xml(result)
    assert "<stages>" in xml
    assert "<module>classifier</module>" in xml


def test_xml_escapes_special_characters():
    result = PipelineResult(
        document_id=uuid4(),
        status="success",
        total_duration_ms=1.0,
        stages=[
            StageResult(
                module="validator", status="success",
                data={"valid": False, "violations": ["amount < 0 & > max"], "warnings": []},
                duration_ms=1.0,
            ),
        ],
    )
    xml = result_to_xml(result)
    assert "&amp;" in xml or "&lt;" in xml  # special chars escaped
    assert "<violations>" in xml


# ---------------------------------------------------------------------------
# API endpoint tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_export_markdown_status_200():
    async with _client() as c:
        r = await c.post("/process", files={"file": ("inv.txt", _INVOICE, "text/plain")})
        doc_id = r.json()["document_id"]
        r2 = await c.get(f"/results/{doc_id}/export?format=markdown")
    assert r2.status_code == 200


@pytest.mark.asyncio
async def test_export_markdown_content_type():
    async with _client() as c:
        r = await c.post("/process", files={"file": ("inv.txt", _INVOICE, "text/plain")})
        doc_id = r.json()["document_id"]
        r2 = await c.get(f"/results/{doc_id}/export?format=markdown")
    assert "markdown" in r2.headers["content-type"]


@pytest.mark.asyncio
async def test_export_markdown_filename_header():
    async with _client() as c:
        r = await c.post("/process", files={"file": ("inv.txt", _INVOICE, "text/plain")})
        doc_id = r.json()["document_id"]
        r2 = await c.get(f"/results/{doc_id}/export?format=markdown")
    assert r2.headers["content-disposition"].endswith('.md"')


@pytest.mark.asyncio
async def test_export_xml_status_200():
    async with _client() as c:
        r = await c.post("/process", files={"file": ("inv.txt", _INVOICE, "text/plain")})
        doc_id = r.json()["document_id"]
        r2 = await c.get(f"/results/{doc_id}/export?format=xml")
    assert r2.status_code == 200


@pytest.mark.asyncio
async def test_export_xml_content_type():
    async with _client() as c:
        r = await c.post("/process", files={"file": ("inv.txt", _INVOICE, "text/plain")})
        doc_id = r.json()["document_id"]
        r2 = await c.get(f"/results/{doc_id}/export?format=xml")
    assert "xml" in r2.headers["content-type"]


@pytest.mark.asyncio
async def test_export_xml_filename_header():
    async with _client() as c:
        r = await c.post("/process", files={"file": ("inv.txt", _INVOICE, "text/plain")})
        doc_id = r.json()["document_id"]
        r2 = await c.get(f"/results/{doc_id}/export?format=xml")
    assert r2.headers["content-disposition"].endswith('.xml"')


@pytest.mark.asyncio
async def test_export_xml_body_has_result_tag():
    async with _client() as c:
        r = await c.post("/process", files={"file": ("inv.txt", _INVOICE, "text/plain")})
        doc_id = r.json()["document_id"]
        r2 = await c.get(f"/results/{doc_id}/export?format=xml")
    assert "<result>" in r2.text
    assert "</result>" in r2.text


@pytest.mark.asyncio
async def test_export_markdown_body_has_heading():
    async with _client() as c:
        r = await c.post("/process", files={"file": ("inv.txt", _INVOICE, "text/plain")})
        doc_id = r.json()["document_id"]
        r2 = await c.get(f"/results/{doc_id}/export?format=markdown")
    assert "# Document Report" in r2.text


@pytest.mark.asyncio
async def test_export_invalid_format_returns_400():
    async with _client() as c:
        r = await c.post("/process", files={"file": ("inv.txt", _INVOICE, "text/plain")})
        doc_id = r.json()["document_id"]
        r2 = await c.get(f"/results/{doc_id}/export?format=pdf")
    assert r2.status_code == 400


@pytest.mark.asyncio
async def test_export_404_for_missing_result():
    async with _client() as c:
        r = await c.get(f"/results/{uuid4()}/export?format=markdown")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_export_json_still_works():
    async with _client() as c:
        r = await c.post("/process", files={"file": ("inv.txt", _INVOICE, "text/plain")})
        doc_id = r.json()["document_id"]
        r2 = await c.get(f"/results/{doc_id}/export?format=json")
    assert r2.status_code == 200
    assert "document_id" in r2.json()


@pytest.mark.asyncio
async def test_export_csv_still_works():
    async with _client() as c:
        r = await c.post("/process", files={"file": ("inv.txt", _INVOICE, "text/plain")})
        doc_id = r.json()["document_id"]
        r2 = await c.get(f"/results/{doc_id}/export?format=csv")
    assert r2.status_code == 200
    assert "document_id" in r2.text
