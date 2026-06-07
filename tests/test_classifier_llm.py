"""Tests for ClassifierModule LLM fallback path (mocked)."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from document_processor.models import Document


def _doc(content: bytes = b"", filename: str | None = None) -> Document:
    return Document(content=content, filename=filename)


@pytest.mark.asyncio
async def test_llm_called_when_confidence_low():
    """LLM is invoked when heuristic confidence falls below threshold."""
    from classifier_module.classifier import ClassifierModule

    clf = ClassifierModule()

    mock_llm = MagicMock()
    mock_llm.available = True
    mock_llm.classify = AsyncMock(return_value=("contract", 0.88))

    with patch("classifier_module.classifier._get_llm", return_value=mock_llm), \
         patch("classifier_module.classifier.settings") as mock_settings:
        mock_settings.llm_enabled = True
        mock_settings.llm_confidence_threshold = 0.50
        mock_settings.llm_max_input_chars = 4000
        mock_settings.llm_vision_enabled = True
        mock_settings.llm_model = "claude-haiku-4-5"

        r = await clf.process(_doc(b"\x00\x01binary"), {"router": {"route": "generic"}})

    assert r.data["type"] == "contract"
    assert r.data["method"] == "llm"
    assert r.data["confidence"] == 0.88
    mock_llm.classify.assert_awaited_once()


@pytest.mark.asyncio
async def test_llm_not_called_when_confidence_high():
    """LLM is NOT invoked when heuristic confidence is above threshold."""
    from classifier_module.classifier import ClassifierModule

    clf = ClassifierModule()
    invoice_text = b"Invoice No: 1234\nBill To: John\nAmount Due: $500\nDue Date: 2026-01-01"

    mock_llm = MagicMock()
    mock_llm.available = True
    mock_llm.classify = AsyncMock(return_value=("invoice", 0.99))

    with patch("classifier_module.classifier._get_llm", return_value=mock_llm), \
         patch("classifier_module.classifier.settings") as mock_settings:
        mock_settings.llm_enabled = True
        mock_settings.llm_confidence_threshold = 0.50
        mock_settings.llm_max_input_chars = 4000
        mock_settings.llm_vision_enabled = True
        mock_settings.llm_model = "claude-haiku-4-5"

        r = await clf.process(_doc(invoice_text), {"router": {"route": "pdf"}})

    # Heuristic should give high confidence for invoice keywords
    assert r.data["type"] == "invoice"
    assert r.data["method"] == "keyword"
    mock_llm.classify.assert_not_awaited()


@pytest.mark.asyncio
async def test_llm_fallback_used_when_unavailable():
    """Falls back to heuristic result when LLM client is unavailable."""
    from classifier_module.classifier import ClassifierModule

    clf = ClassifierModule()

    mock_llm = MagicMock()
    mock_llm.available = False
    mock_llm.classify = AsyncMock()

    with patch("classifier_module.classifier._get_llm", return_value=mock_llm), \
         patch("classifier_module.classifier.settings") as mock_settings:
        mock_settings.llm_enabled = True
        mock_settings.llm_confidence_threshold = 0.50
        mock_settings.llm_max_input_chars = 4000
        mock_settings.llm_vision_enabled = True
        mock_settings.llm_model = "claude-haiku-4-5"

        r = await clf.process(_doc(b""), {"router": {"route": "generic"}})

    assert r.status == "success"
    mock_llm.classify.assert_not_awaited()


@pytest.mark.asyncio
async def test_llm_disabled_by_setting():
    """When llm_enabled=False, LLM is never called."""
    from classifier_module.classifier import ClassifierModule

    clf = ClassifierModule()

    mock_llm = MagicMock()
    mock_llm.available = True
    mock_llm.classify = AsyncMock(return_value=("invoice", 0.95))

    with patch("classifier_module.classifier._get_llm", return_value=mock_llm), \
         patch("classifier_module.classifier.settings") as mock_settings:
        mock_settings.llm_enabled = False
        mock_settings.llm_confidence_threshold = 0.50

        r = await clf.process(_doc(b""), {"router": {"route": "generic"}})

    mock_llm.classify.assert_not_awaited()
    assert r.status == "success"


@pytest.mark.asyncio
async def test_llm_returns_none_gracefully():
    """If LLM call returns None, keep heuristic result."""
    from classifier_module.classifier import ClassifierModule

    clf = ClassifierModule()

    mock_llm = MagicMock()
    mock_llm.available = True
    mock_llm.classify = AsyncMock(return_value=None)

    with patch("classifier_module.classifier._get_llm", return_value=mock_llm), \
         patch("classifier_module.classifier.settings") as mock_settings:
        mock_settings.llm_enabled = True
        mock_settings.llm_confidence_threshold = 0.50
        mock_settings.llm_max_input_chars = 4000
        mock_settings.llm_vision_enabled = True
        mock_settings.llm_model = "claude-haiku-4-5"

        r = await clf.process(_doc(b""), {"router": {"route": "generic"}})

    assert r.status == "success"
    assert r.data["method"] != "llm"  # heuristic result preserved
