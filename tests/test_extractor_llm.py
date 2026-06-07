"""Tests for ExtractorModule LLM enrichment path (mocked)."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from document_processor.models import Document


def _doc(content: bytes, mimetype: str = "text/plain") -> Document:
    return Document(content=content, mimetype=mimetype)


def _ctx(route: str, doc_type: str = "unknown") -> dict:
    return {
        "router": {"route": route},
        "classifier": {"type": doc_type},
    }


@pytest.mark.asyncio
async def test_llm_enriches_invoice_fields():
    """LLM-extracted parties and reference numbers are merged into fields."""
    from extractor_module.extractor import ExtractorModule

    ext = ExtractorModule()
    content = b"Invoice 2026-INV-001 for Acme Corp. Total: $500."

    mock_llm = MagicMock()
    mock_llm.available = True
    mock_llm.extract = AsyncMock(return_value={
        "parties": ["Acme Corp"],
        "reference_numbers": ["2026-INV-001"],
        "amounts": [500.0],
    })

    with patch("extractor_module.extractor._get_llm", return_value=mock_llm), \
         patch("extractor_module.extractor.settings") as mock_settings:
        mock_settings.llm_enabled = True
        mock_settings.llm_max_input_chars = 4000
        mock_settings.llm_vision_enabled = True
        mock_settings.llm_model = "claude-haiku-4-5"

        r = await ext.process(_doc(content), _ctx("pdf", "invoice"))

    assert "parties" in r.data["fields"]
    assert "Acme Corp" in r.data["fields"]["parties"]
    assert "reference_numbers" in r.data["fields"]
    mock_llm.extract.assert_awaited_once()


@pytest.mark.asyncio
async def test_llm_merges_amounts_without_duplicates():
    """Regex amounts and LLM amounts are merged without duplicates."""
    from extractor_module.extractor import ExtractorModule

    ext = ExtractorModule()
    content = b"Total: $100.00"  # regex will find 100.00

    mock_llm = MagicMock()
    mock_llm.available = True
    mock_llm.extract = AsyncMock(return_value={
        "amounts": [100.0, 200.0],  # 100.0 overlaps with regex result
    })

    with patch("extractor_module.extractor._get_llm", return_value=mock_llm), \
         patch("extractor_module.extractor.settings") as mock_settings:
        mock_settings.llm_enabled = True
        mock_settings.llm_max_input_chars = 4000
        mock_settings.llm_vision_enabled = True
        mock_settings.llm_model = "claude-haiku-4-5"

        r = await ext.process(_doc(content), _ctx("pdf", "invoice"))

    amounts = r.data["fields"].get("amounts", [])
    assert len([a for a in amounts if str(a) == "100.0"]) == 1  # no duplicate


@pytest.mark.asyncio
async def test_llm_not_called_for_non_enriched_type():
    """LLM is skipped for unknown/spreadsheet/etc. doc types."""
    from extractor_module.extractor import ExtractorModule

    ext = ExtractorModule()

    mock_llm = MagicMock()
    mock_llm.available = True
    mock_llm.extract = AsyncMock(return_value={"amounts": [1.0]})

    with patch("extractor_module.extractor._get_llm", return_value=mock_llm), \
         patch("extractor_module.extractor.settings") as mock_settings:
        mock_settings.llm_enabled = True
        mock_settings.llm_max_input_chars = 4000
        mock_settings.llm_vision_enabled = True
        mock_settings.llm_model = "claude-haiku-4-5"

        r = await ext.process(_doc(b"hello world"), _ctx("text", "unknown"))

    mock_llm.extract.assert_not_awaited()
    assert r.status == "success"


@pytest.mark.asyncio
async def test_llm_disabled_by_setting():
    """When llm_enabled=False, LLM is never called."""
    from extractor_module.extractor import ExtractorModule

    ext = ExtractorModule()

    mock_llm = MagicMock()
    mock_llm.available = True
    mock_llm.extract = AsyncMock(return_value={"amounts": [1.0]})

    with patch("extractor_module.extractor._get_llm", return_value=mock_llm), \
         patch("extractor_module.extractor.settings") as mock_settings:
        mock_settings.llm_enabled = False

        r = await ext.process(_doc(b"Invoice text"), _ctx("pdf", "invoice"))

    mock_llm.extract.assert_not_awaited()
    assert r.status == "success"


@pytest.mark.asyncio
async def test_llm_unavailable_gracefully():
    """Falls back to regex-only when LLM client is unavailable."""
    from extractor_module.extractor import ExtractorModule

    ext = ExtractorModule()
    content = b"Email: support@example.com"

    mock_llm = MagicMock()
    mock_llm.available = False
    mock_llm.extract = AsyncMock()

    with patch("extractor_module.extractor._get_llm", return_value=mock_llm), \
         patch("extractor_module.extractor.settings") as mock_settings:
        mock_settings.llm_enabled = True
        mock_settings.llm_max_input_chars = 4000
        mock_settings.llm_vision_enabled = True
        mock_settings.llm_model = "claude-haiku-4-5"

        r = await ext.process(_doc(content), _ctx("pdf", "invoice"))

    mock_llm.extract.assert_not_awaited()
    assert "support@example.com" in r.data["fields"].get("emails", [])


@pytest.mark.asyncio
async def test_image_route_triggers_llm():
    """Image route triggers LLM extraction regardless of doc type."""
    from extractor_module.extractor import ExtractorModule

    ext = ExtractorModule()

    mock_llm = MagicMock()
    mock_llm.available = True
    mock_llm.extract = AsyncMock(return_value={"amounts": [42.0]})

    with patch("extractor_module.extractor._get_llm", return_value=mock_llm), \
         patch("extractor_module.extractor.settings") as mock_settings:
        mock_settings.llm_enabled = True
        mock_settings.llm_max_input_chars = 4000
        mock_settings.llm_vision_enabled = True
        mock_settings.llm_model = "claude-haiku-4-5"

        r = await ext.process(
            Document(content=b"\xff\xd8\xff", mimetype="image/jpeg"),
            _ctx("image", "image"),
        )

    mock_llm.extract.assert_awaited_once()
    assert r.status == "success"
