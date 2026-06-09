"""Tests for improved extractor: YAML/XML/DOCX/TSV/email, metadata, new patterns."""
from __future__ import annotations

import csv
import io
import json
import xml.etree.ElementTree as ET
import zipfile

import pytest

from document_processor.models import Document
from extractor_module.extractor import ExtractorModule
from extractor_module.patterns import FIELD_PATTERNS

ext = ExtractorModule()


def _doc(content: bytes, filename: str = "", mimetype: str = "") -> Document:
    return Document(content=content, filename=filename, mimetype=mimetype)


def _ctx(route: str = "generic", doc_type: str = "unknown") -> dict:
    return {
        "router": {"route": route},
        "classifier": {"type": doc_type},
        "_config": {"llm_enabled": False},
    }


# ---------------------------------------------------------------------------
# New patterns
# ---------------------------------------------------------------------------

def test_swift_bic_pattern():
    text = "Bank SWIFT: DEUTDEDB"
    matches = FIELD_PATTERNS["swift_bic"].findall(text)
    assert any("DEUTDEDB" in m for m in matches)


def test_sort_code_pattern():
    matches = FIELD_PATTERNS["sort_codes"].findall("Sort Code: 20-00-00")
    assert "20-00-00" in matches


def test_company_registration_pattern():
    text = "Company No. 12345678 registered in England"
    matches = FIELD_PATTERNS["company_registration"].findall(text)
    assert any("12345678" in str(m) for m in matches)


def test_amounts_additional_currencies():
    for symbol in ["₩", "₫", "₴"]:
        text = f"Total: {symbol}500.00"
        matches = FIELD_PATTERNS["amounts"].findall(text)
        assert matches, f"No match for {symbol}"


def test_amounts_more_iso_codes():
    for code in ["NZD", "SGD", "SEK", "NOK", "DKK", "HKD"]:
        text = f"Amount: 100.00 {code}"
        matches = FIELD_PATTERNS["amounts"].findall(text)
        assert matches, f"No match for ISO code {code}"


def test_dates_dot_notation():
    matches = FIELD_PATTERNS["dates"].findall("Date: 31.01.2024")
    assert any("31.01.2024" in m for m in matches)


def test_dates_month_name_leading():
    matches = FIELD_PATTERNS["dates"].findall("January 31, 2024")
    assert matches


def test_reference_numbers_rma():
    matches = FIELD_PATTERNS["reference_numbers"].findall("Return: RMA-20240101")
    assert matches


def test_reference_numbers_dn():
    matches = FIELD_PATTERNS["reference_numbers"].findall("Delivery: DN-2024001")
    assert matches


# ---------------------------------------------------------------------------
# YAML extraction
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_yaml_extraction():
    content = b"name: Acme Corp\ntype: supplier\nactive: true\n"
    doc = _doc(content, filename="config.yaml", mimetype="application/yaml")
    r = await ext.process(doc, _ctx("yaml"))
    assert r.status == "success"
    assert r.data["raw_text"] != "" or "yaml_data" in r.data["fields"]


@pytest.mark.asyncio
async def test_yaml_no_library_falls_back():
    # Even without pyyaml, should return the raw text
    content = b"key: value\nother: 123\n"
    doc = _doc(content)
    r = await ext.process(doc, _ctx("yaml"))
    assert r.status == "success"
    assert "key" in r.data["raw_text"] or r.data["raw_text"] == ""


# ---------------------------------------------------------------------------
# XML extraction
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_xml_text_extraction():
    xml = b"<?xml version='1.0'?><root><item>Hello</item><item>World</item></root>"
    doc = _doc(xml, mimetype="application/xml")
    r = await ext.process(doc, _ctx("xml"))
    assert r.status == "success"
    assert "Hello" in r.data["raw_text"]
    assert "World" in r.data["raw_text"]


@pytest.mark.asyncio
async def test_xml_invalid_falls_back_to_text():
    content = b"not valid xml <<<"
    doc = _doc(content)
    r = await ext.process(doc, _ctx("xml"))
    assert r.status == "success"


# ---------------------------------------------------------------------------
# DOCX extraction
# ---------------------------------------------------------------------------

def _make_docx(text: str) -> bytes:
    ns = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
    root = ET.Element(f"{{{ns}}}document")
    body = ET.SubElement(root, f"{{{ns}}}body")
    para = ET.SubElement(body, f"{{{ns}}}p")
    run = ET.SubElement(para, f"{{{ns}}}r")
    t_elem = ET.SubElement(run, f"{{{ns}}}t")
    t_elem.text = text
    xml_bytes = ET.tostring(root, encoding="unicode").encode()

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("word/document.xml", xml_bytes)
        zf.writestr("[Content_Types].xml", b"<Types/>")
    buf.seek(0)
    return buf.read()


@pytest.mark.asyncio
async def test_docx_text_extraction():
    docx = _make_docx("Invoice Number: INV-2024-001")
    doc = _doc(docx, filename="doc.docx", mimetype="application/vnd.openxmlformats-officedocument.wordprocessingml.document")
    r = await ext.process(doc, _ctx("word"))
    assert "INV-2024-001" in r.data["raw_text"] or "INV" in str(r.data["fields"])


@pytest.mark.asyncio
async def test_docx_non_zip_falls_back():
    doc = _doc(b"plain text fallback", filename="doc.docx")
    r = await ext.process(doc, _ctx("word"))
    assert r.status == "success"
    assert "plain text fallback" in r.data["raw_text"]


# ---------------------------------------------------------------------------
# TSV extraction
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_tsv_extraction_by_extension():
    content = b"name\tage\tcity\nAlice\t30\tLondon\nBob\t25\tParis\n"
    doc = _doc(content, filename="data.tsv")
    r = await ext.process(doc, _ctx("spreadsheet"))
    assert r.status == "success"
    assert r.data["fields"].get("columns") == ["name", "age", "city"]
    assert r.data["fields"].get("row_count") == 2


@pytest.mark.asyncio
async def test_csv_extraction_sniff_delimiter():
    content = b"id,name,amount\n1,Alice,100.00\n2,Bob,200.00\n"
    doc = _doc(content, filename="data.csv")
    r = await ext.process(doc, _ctx("spreadsheet"))
    assert r.status == "success"
    assert "id" in r.data["fields"].get("columns", [])
    assert r.data["fields"].get("row_count") == 2


@pytest.mark.asyncio
async def test_csv_sample_rows():
    rows = "\n".join(f"{i},val{i}" for i in range(10))
    content = f"id,val\n{rows}\n".encode()
    doc = _doc(content, filename="data.csv")
    r = await ext.process(doc, _ctx("spreadsheet"))
    assert len(r.data["fields"].get("sample_rows", [])) <= 5


# ---------------------------------------------------------------------------
# Email extraction
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_email_header_parsing():
    eml = (
        b"From: alice@example.com\r\n"
        b"To: bob@example.com\r\n"
        b"Subject: Invoice #INV-001\r\n"
        b"Date: Mon, 01 Jan 2024 10:00:00 +0000\r\n"
        b"\r\n"
        b"Please find attached the invoice.\r\n"
    )
    doc = _doc(eml, filename="message.eml")
    r = await ext.process(doc, _ctx("email"))
    assert r.status == "success"
    headers = r.data["fields"].get("email_headers", {})
    assert headers.get("from") == "alice@example.com"
    assert headers.get("subject") == "Invoice #INV-001"


@pytest.mark.asyncio
async def test_email_body_extracted():
    eml = b"From: x@y.com\r\nSubject: test\r\n\r\nHello world\r\n"
    doc = _doc(eml, filename="msg.eml")
    r = await ext.process(doc, _ctx("email"))
    assert "Hello world" in r.data["raw_text"]


# ---------------------------------------------------------------------------
# Metadata improvements
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_metadata_includes_char_count():
    doc = _doc(b"hello world", filename="test.txt")
    r = await ext.process(doc, _ctx("text"))
    assert "char_count" in r.data["metadata"]
    assert r.data["metadata"]["char_count"] == len("hello world")


@pytest.mark.asyncio
async def test_metadata_includes_size_bytes():
    content = b"some content here"
    doc = _doc(content, filename="test.txt")
    r = await ext.process(doc, _ctx("text"))
    assert r.data["metadata"]["size_bytes"] == len(content)


@pytest.mark.asyncio
async def test_metadata_filename_and_mimetype():
    doc = _doc(b"", filename="invoice.pdf", mimetype="application/pdf")
    r = await ext.process(doc, _ctx("pdf"))
    assert r.data["metadata"]["filename"] == "invoice.pdf"
    assert r.data["metadata"]["mimetype"] == "application/pdf"


# ---------------------------------------------------------------------------
# NDJSON extraction
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_ndjson_extraction():
    content = b'{"id":1,"name":"Alice"}\n{"id":2,"name":"Bob"}\n'
    doc = _doc(content, filename="records.ndjson")
    r = await ext.process(doc, _ctx("json"))
    assert r.status == "success"
    json_data = r.data["fields"].get("json_data")
    assert json_data is not None
    assert isinstance(json_data, list)
    assert len(json_data) == 2


# ---------------------------------------------------------------------------
# Latin-1 fallback encoding
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_latin1_encoded_file():
    # € is not in latin-1; use a latin-1 safe character like £ (0xa3)
    content = "Prix: 100\xa3 Date: 01/01/2024".encode("latin-1")
    doc = _doc(content, filename="doc.txt")
    r = await ext.process(doc, _ctx("text"))
    assert r.status == "success"
    assert r.data["raw_text"] != ""


# ---------------------------------------------------------------------------
# Integration
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_process_status_success():
    doc = _doc(b"any content", filename="test.txt")
    r = await ext.process(doc, _ctx())
    assert r.status == "success"
    assert r.module == "extractor"


@pytest.mark.asyncio
async def test_process_fields_and_metadata_present():
    doc = _doc(b"Invoice 2024-01-01 $500.00", filename="invoice.txt")
    r = await ext.process(doc, _ctx("generic", "invoice"))
    assert "fields" in r.data
    assert "metadata" in r.data
