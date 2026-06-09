from __future__ import annotations

import base64
import json
import logging
from contextvars import ContextVar
from typing import Any

_log = logging.getLogger(__name__)

# Per-async-task LLM usage accumulator — safe under concurrent requests.
_usage_accumulator: ContextVar[list[dict] | None] = ContextVar("_llm_usage", default=None)


def start_usage_tracking() -> None:
    """Reset the usage accumulator for the current async task (call before each pipeline run)."""
    _usage_accumulator.set([])


def get_llm_usage() -> list[dict]:
    """Return accumulated LLM usage entries for the current async task."""
    return _usage_accumulator.get(None) or []

try:
    import anthropic as _anthropic

    _sdk_available = True
except ImportError:
    _anthropic = None  # type: ignore[assignment]
    _sdk_available = False


class LLMClient:
    """Thin async wrapper around the Anthropic SDK with graceful fallback."""

    def __init__(self, model: str, max_input_chars: int = 4_000) -> None:
        self.model = model
        self.max_input_chars = max_input_chars
        self._client: Any = None
        if _sdk_available:
            try:
                self._client = _anthropic.AsyncAnthropic()
            except Exception as exc:
                _log.warning("LLMClient: could not create AsyncAnthropic client: %s", exc)

    @property
    def available(self) -> bool:
        return self._client is not None

    # ------------------------------------------------------------------
    # Classification
    # ------------------------------------------------------------------

    async def classify(
        self,
        text: str,
        known_types: list[str],
        image_bytes: bytes | None = None,
        image_media_type: str = "image/jpeg",
    ) -> tuple[str, float] | None:
        """Return (doc_type, confidence) or None on failure."""
        if not self.available:
            return None

        tool_schema = {
            "name": "set_document_type",
            "description": "Classify the document into exactly one type.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "document_type": {
                        "type": "string",
                        "enum": known_types,
                        "description": "The document type.",
                    },
                    "confidence": {
                        "type": "number",
                        "description": "Confidence between 0.0 and 1.0.",
                    },
                    "reasoning": {
                        "type": "string",
                        "description": "One sentence explaining the classification.",
                    },
                },
                "required": ["document_type", "confidence", "reasoning"],
            },
        }

        content: list[dict] = []
        if image_bytes:
            content.append({
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": image_media_type,
                    "data": base64.standard_b64encode(image_bytes).decode(),
                },
            })
        if text:
            content.append({"type": "text", "text": text[: self.max_input_chars]})
        if not content:
            content.append({"type": "text", "text": "(empty document)"})

        try:
            response = await self._client.messages.create(
                model=self.model,
                max_tokens=256,
                tools=[tool_schema],
                tool_choice={"type": "tool", "name": "set_document_type"},
                messages=[{"role": "user", "content": content}],
            )
            acc = _usage_accumulator.get(None)
            if acc is not None and hasattr(response, "usage") and response.usage:
                acc.append({
                    "stage": "classifier",
                    "model": self.model,
                    "input_tokens": getattr(response.usage, "input_tokens", 0),
                    "output_tokens": getattr(response.usage, "output_tokens", 0),
                })
            for block in response.content:
                if block.type == "tool_use" and block.name == "set_document_type":
                    inp = block.input
                    doc_type = inp.get("document_type", "unknown")
                    confidence = float(inp.get("confidence", 0.5))
                    return doc_type, min(max(confidence, 0.0), 1.0)
        except Exception as exc:
            _log.warning("LLMClient.classify failed: %s", exc)

        return None

    # ------------------------------------------------------------------
    # Extraction
    # ------------------------------------------------------------------

    async def extract(
        self,
        text: str,
        doc_type: str,
        image_bytes: bytes | None = None,
        image_media_type: str = "image/jpeg",
    ) -> dict[str, Any] | None:
        """Return extracted fields dict or None on failure."""
        if not self.available:
            return None

        tool_schema = {
            "name": "set_extracted_fields",
            "description": "Extract structured fields from the document.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "amounts": {
                        "type": "array",
                        "items": {"type": "number"},
                        "description": "Monetary amounts as numbers.",
                    },
                    "dates": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Dates in ISO-8601 (YYYY-MM-DD) format.",
                    },
                    "emails": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Email addresses.",
                    },
                    "parties": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Named individuals or organisations mentioned.",
                    },
                    "reference_numbers": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Invoice/PO/contract reference numbers.",
                    },
                    "key_terms": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Important terms or clauses (for contracts/specs).",
                    },
                },
                "required": [],
            },
        }

        content: list[dict] = []
        if image_bytes:
            content.append({
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": image_media_type,
                    "data": base64.standard_b64encode(image_bytes).decode(),
                },
            })
        if text:
            content.append({
                "type": "text",
                "text": (
                    f"Document type: {doc_type}\n\n"
                    + text[: self.max_input_chars]
                ),
            })
        if not content:
            content.append({"type": "text", "text": f"Document type: {doc_type}\n\n(empty)"})

        try:
            response = await self._client.messages.create(
                model=self.model,
                max_tokens=1024,
                tools=[tool_schema],
                tool_choice={"type": "tool", "name": "set_extracted_fields"},
                messages=[{"role": "user", "content": content}],
            )
            acc = _usage_accumulator.get(None)
            if acc is not None and hasattr(response, "usage") and response.usage:
                acc.append({
                    "stage": "extractor",
                    "model": self.model,
                    "input_tokens": getattr(response.usage, "input_tokens", 0),
                    "output_tokens": getattr(response.usage, "output_tokens", 0),
                })
            for block in response.content:
                if block.type == "tool_use" and block.name == "set_extracted_fields":
                    return {k: v for k, v in block.input.items() if v}
        except Exception as exc:
            _log.warning("LLMClient.extract failed: %s", exc)

        return None
