"""Tests for LLMClient — all Anthropic SDK calls are mocked."""
from __future__ import annotations

import sys
import types
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Build a minimal stub for the `anthropic` module so LLMClient can import
# without the real SDK installed (CI may not have ANTHROPIC_API_KEY).
# ---------------------------------------------------------------------------

def _make_tool_use_block(name: str, input_data: dict):
    block = MagicMock()
    block.type = "tool_use"
    block.name = name
    block.input = input_data
    return block


def _make_response(blocks):
    resp = MagicMock()
    resp.content = blocks
    return resp


# ---------------------------------------------------------------------------
# Tests for classify()
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_classify_returns_type_and_confidence():
    from llm_client.client import LLMClient

    client = LLMClient(model="claude-haiku-4-5")
    mock_resp = _make_response([
        _make_tool_use_block("set_document_type", {
            "document_type": "invoice",
            "confidence": 0.92,
            "reasoning": "Contains invoice keywords.",
        })
    ])

    mock_messages = AsyncMock()
    mock_messages.create = AsyncMock(return_value=mock_resp)
    mock_api = MagicMock()
    mock_api.messages = mock_messages
    client._client = mock_api

    result = await client.classify(text="Invoice No: 1234", known_types=["invoice", "contract", "unknown"])
    assert result == ("invoice", 0.92)


@pytest.mark.asyncio
async def test_classify_clamps_confidence():
    from llm_client.client import LLMClient

    client = LLMClient(model="claude-haiku-4-5")
    mock_resp = _make_response([
        _make_tool_use_block("set_document_type", {
            "document_type": "contract",
            "confidence": 1.5,  # out of range
            "reasoning": "Clearly a contract.",
        })
    ])

    mock_messages = AsyncMock()
    mock_messages.create = AsyncMock(return_value=mock_resp)
    mock_api = MagicMock()
    mock_api.messages = mock_messages
    client._client = mock_api

    result = await client.classify(text="Agreement between parties.", known_types=["invoice", "contract", "unknown"])
    assert result is not None
    _, confidence = result
    assert confidence <= 1.0


@pytest.mark.asyncio
async def test_classify_returns_none_on_sdk_error():
    from llm_client.client import LLMClient

    client = LLMClient(model="claude-haiku-4-5")
    mock_messages = AsyncMock()
    mock_messages.create = AsyncMock(side_effect=Exception("API error"))
    mock_api = MagicMock()
    mock_api.messages = mock_messages
    client._client = mock_api

    result = await client.classify(text="some text", known_types=["invoice", "unknown"])
    assert result is None


@pytest.mark.asyncio
async def test_classify_returns_none_when_no_client():
    from llm_client.client import LLMClient

    client = LLMClient(model="claude-haiku-4-5")
    client._client = None  # simulate missing API key

    result = await client.classify(text="hello", known_types=["invoice", "unknown"])
    assert result is None


@pytest.mark.asyncio
async def test_classify_with_image_bytes():
    from llm_client.client import LLMClient

    client = LLMClient(model="claude-haiku-4-5")
    mock_resp = _make_response([
        _make_tool_use_block("set_document_type", {
            "document_type": "image",
            "confidence": 0.88,
            "reasoning": "It is an image.",
        })
    ])

    mock_messages = AsyncMock()
    mock_messages.create = AsyncMock(return_value=mock_resp)
    mock_api = MagicMock()
    mock_api.messages = mock_messages
    client._client = mock_api

    result = await client.classify(
        text="",
        known_types=["image", "unknown"],
        image_bytes=b"\xff\xd8\xff",
        image_media_type="image/jpeg",
    )
    assert result == ("image", 0.88)
    # Verify the image was included in the messages.create call
    call_kwargs = mock_messages.create.call_args[1]
    content = call_kwargs["messages"][0]["content"]
    image_blocks = [b for b in content if b.get("type") == "image"]
    assert len(image_blocks) == 1


# ---------------------------------------------------------------------------
# Tests for extract()
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_extract_returns_fields():
    from llm_client.client import LLMClient

    client = LLMClient(model="claude-haiku-4-5")
    mock_resp = _make_response([
        _make_tool_use_block("set_extracted_fields", {
            "amounts": [1250.0],
            "dates": ["2026-01-15"],
            "parties": ["Acme Corp"],
        })
    ])

    mock_messages = AsyncMock()
    mock_messages.create = AsyncMock(return_value=mock_resp)
    mock_api = MagicMock()
    mock_api.messages = mock_messages
    client._client = mock_api

    result = await client.extract(text="Invoice for $1,250.00 dated 2026-01-15", doc_type="invoice")
    assert result is not None
    assert result["amounts"] == [1250.0]
    assert "2026-01-15" in result["dates"]
    assert "Acme Corp" in result["parties"]


@pytest.mark.asyncio
async def test_extract_filters_empty_values():
    from llm_client.client import LLMClient

    client = LLMClient(model="claude-haiku-4-5")
    mock_resp = _make_response([
        _make_tool_use_block("set_extracted_fields", {
            "amounts": [99.0],
            "dates": [],   # empty list should be filtered out
            "emails": [],
        })
    ])

    mock_messages = AsyncMock()
    mock_messages.create = AsyncMock(return_value=mock_resp)
    mock_api = MagicMock()
    mock_api.messages = mock_messages
    client._client = mock_api

    result = await client.extract(text="Pay $99.00", doc_type="receipt")
    assert result is not None
    assert "amounts" in result
    assert "dates" not in result   # empty list filtered
    assert "emails" not in result


@pytest.mark.asyncio
async def test_extract_returns_none_on_error():
    from llm_client.client import LLMClient

    client = LLMClient(model="claude-haiku-4-5")
    mock_messages = AsyncMock()
    mock_messages.create = AsyncMock(side_effect=RuntimeError("timeout"))
    mock_api = MagicMock()
    mock_api.messages = mock_messages
    client._client = mock_api

    result = await client.extract(text="some text", doc_type="invoice")
    assert result is None


@pytest.mark.asyncio
async def test_extract_returns_none_when_no_client():
    from llm_client.client import LLMClient

    client = LLMClient(model="claude-haiku-4-5")
    client._client = None

    result = await client.extract(text="hello", doc_type="letter")
    assert result is None
