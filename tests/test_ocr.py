"""Tests for OCR/PDF extraction paths in ExtractorModule."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from document_processor.models import Document
from extractor_module.extractor import ExtractorModule

ext = ExtractorModule()


def _doc(content: bytes, mimetype: str = "application/pdf") -> Document:
    return Document(content=content, mimetype=mimetype)


@pytest.mark.asyncio
async def test_pdf_route_uses_pdfplumber_when_available():
    mock_pdf = MagicMock()
    mock_page = MagicMock()
    mock_page.extract_text.return_value = "Invoice No: 42\nAmount Due: $500"
    mock_pdf.pages = [mock_page]
    mock_pdf.__enter__ = lambda s: mock_pdf
    mock_pdf.__exit__ = MagicMock(return_value=False)

    mock_pdfplumber = MagicMock()
    mock_pdfplumber.open.return_value = mock_pdf

    with patch("extractor_module.extractor._pdfplumber", mock_pdfplumber), \
         patch("extractor_module.extractor.settings") as s:
        s.llm_enabled = False
        r = await ext.process(_doc(b"%PDF-1.4 fake"), {"router": {"route": "pdf"}, "classifier": {"type": "invoice"}})

    assert "Invoice No: 42" in r.data["raw_text"]
    # open() is called twice: once for text extraction, once for page-count metadata
    assert mock_pdfplumber.open.call_count >= 1


@pytest.mark.asyncio
async def test_pdf_route_falls_back_when_pdfplumber_absent():
    with patch("extractor_module.extractor._pdfplumber", None), \
         patch("extractor_module.extractor.settings") as s:
        s.llm_enabled = False
        r = await ext.process(
            _doc(b"plain text content"),
            {"router": {"route": "pdf"}, "classifier": {"type": "unknown"}},
        )
    assert r.status == "success"
    assert "plain text content" in r.data["raw_text"]


@pytest.mark.asyncio
async def test_pdf_route_falls_back_on_pdfplumber_error():
    mock_pdfplumber = MagicMock()
    mock_pdfplumber.open.side_effect = Exception("corrupt pdf")

    with patch("extractor_module.extractor._pdfplumber", mock_pdfplumber), \
         patch("extractor_module.extractor.settings") as s:
        s.llm_enabled = False
        r = await ext.process(
            _doc(b"fallback text"),
            {"router": {"route": "pdf"}, "classifier": {"type": "unknown"}},
        )
    assert r.status == "success"


@pytest.mark.asyncio
async def test_image_route_uses_pytesseract_when_available():
    mock_pytesseract = MagicMock()
    mock_pytesseract.image_to_string.return_value = "OCR extracted text"
    mock_pil = MagicMock()
    mock_pil.open.return_value = MagicMock()

    with patch("extractor_module.extractor._pytesseract", mock_pytesseract), \
         patch("extractor_module.extractor._PILImage", mock_pil), \
         patch("extractor_module.extractor.settings") as s:
        s.llm_enabled = False
        r = await ext.process(
            Document(content=b"\xff\xd8\xff", mimetype="image/jpeg"),
            {"router": {"route": "image"}, "classifier": {"type": "image"}},
        )

    assert "OCR extracted text" in r.data["raw_text"]
    mock_pytesseract.image_to_string.assert_called_once()


@pytest.mark.asyncio
async def test_image_route_returns_empty_when_ocr_absent():
    with patch("extractor_module.extractor._pytesseract", None), \
         patch("extractor_module.extractor._PILImage", None), \
         patch("extractor_module.extractor.settings") as s:
        s.llm_enabled = False
        r = await ext.process(
            Document(content=b"\xff\xd8\xff", mimetype="image/jpeg"),
            {"router": {"route": "image"}, "classifier": {"type": "image"}},
        )
    assert r.status == "success"
    assert r.data["raw_text"] == ""


@pytest.mark.asyncio
async def test_pdfplumber_multipage_joined():
    mock_pdf = MagicMock()
    pages = [MagicMock(), MagicMock(), MagicMock()]
    pages[0].extract_text.return_value = "Page one"
    pages[1].extract_text.return_value = "Page two"
    pages[2].extract_text.return_value = None  # blank page
    mock_pdf.pages = pages
    mock_pdf.__enter__ = lambda s: mock_pdf
    mock_pdf.__exit__ = MagicMock(return_value=False)

    mock_pdfplumber = MagicMock()
    mock_pdfplumber.open.return_value = mock_pdf

    with patch("extractor_module.extractor._pdfplumber", mock_pdfplumber), \
         patch("extractor_module.extractor.settings") as s:
        s.llm_enabled = False
        r = await ext.process(_doc(b"%PDF"), {"router": {"route": "pdf"}, "classifier": {"type": "unknown"}})

    assert "Page one" in r.data["raw_text"]
    assert "Page two" in r.data["raw_text"]
