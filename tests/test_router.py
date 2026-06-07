import io
import json
import struct
import zipfile

import pytest

from document_processor.models import Document
from router_module import RouterModule

router = RouterModule()


def _doc(content: bytes, mimetype: str = "application/octet-stream", filename: str | None = None) -> Document:
    return Document(content=content, mimetype=mimetype, filename=filename)


# PDF
@pytest.mark.asyncio
async def test_routes_pdf_by_magic():
    doc = _doc(b"%PDF-1.7 fake content")
    r = await router.process(doc, {})
    assert r.data["route"] == "pdf"
    assert r.data["method"] == "magic_bytes"
    assert r.data["confidence"] >= 0.9


# JPEG
@pytest.mark.asyncio
async def test_routes_jpeg_by_magic():
    doc = _doc(b"\xff\xd8\xff\xe0 fake jpeg")
    r = await router.process(doc, {})
    assert r.data["route"] == "image"
    assert r.data["method"] == "magic_bytes"


# PNG
@pytest.mark.asyncio
async def test_routes_png_by_magic():
    doc = _doc(b"\x89PNG\r\n\x1a\n fake png")
    r = await router.process(doc, {})
    assert r.data["route"] == "image"


# WebP (RIFF....WEBP)
@pytest.mark.asyncio
async def test_routes_webp_by_magic():
    content = b"RIFF" + struct.pack("<I", 100) + b"WEBP" + b"\x00" * 90
    doc = _doc(content)
    r = await router.process(doc, {})
    assert r.data["route"] == "image"


# JSON
@pytest.mark.asyncio
async def test_routes_json_by_content():
    doc = _doc(json.dumps({"key": "value"}).encode())
    r = await router.process(doc, {})
    assert r.data["route"] == "json"


# DOCX (ZIP-based)
@pytest.mark.asyncio
async def test_routes_docx_by_zip_content():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("word/document.xml", "<w:document/>")
        zf.writestr("[Content_Types].xml", "")
    doc = _doc(buf.getvalue(), filename="report.docx")
    r = await router.process(doc, {})
    assert r.data["route"] == "word"
    assert r.data["method"] == "zip_content"


# XLSX
@pytest.mark.asyncio
async def test_routes_xlsx_by_zip_content():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("xl/workbook.xml", "")
    doc = _doc(buf.getvalue(), filename="data.xlsx")
    r = await router.process(doc, {})
    assert r.data["route"] == "spreadsheet"


# Fallback to MIME type
@pytest.mark.asyncio
async def test_falls_back_to_declared_mimetype():
    doc = _doc(b"unknown bytes", mimetype="text/csv")
    r = await router.process(doc, {})
    assert r.data["route"] == "spreadsheet"
    assert r.data["method"] == "declared_mimetype"


# Fallback to extension
@pytest.mark.asyncio
async def test_falls_back_to_extension():
    doc = _doc(b"unknown bytes", filename="archive.tar")
    r = await router.process(doc, {})
    assert r.data["route"] == "archive"
    assert r.data["method"] == "file_extension"


# Generic fallback
@pytest.mark.asyncio
async def test_generic_fallback():
    doc = _doc(b"\x00\x01\x02\x03 truly unknown")
    r = await router.process(doc, {})
    assert r.data["route"] == "generic"
    assert r.data["confidence"] == 0.0


# Status is always success
@pytest.mark.asyncio
async def test_always_succeeds():
    doc = _doc(b"")
    r = await router.process(doc, {})
    assert r.status == "success"
    assert r.module == "router"
