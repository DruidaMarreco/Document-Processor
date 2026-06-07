from __future__ import annotations

import csv
import io
import json
import time
from html.parser import HTMLParser

from document_processor.config import settings
from document_processor.models import Document, StageResult

from extractor_module.patterns import FIELD_PATTERNS

_MAX_TEXT = 8_000  # chars kept in context for downstream stages

# Doc types where LLM extraction adds meaningful value beyond regex
_LLM_ENRICHED_TYPES = {
    "invoice", "receipt", "purchase_order", "contract",
    "statement", "report", "specification", "letter",
}

_llm: object = None


def _get_llm():
    global _llm
    if _llm is None:
        from llm_client import LLMClient
        _llm = LLMClient(
            model=settings.llm_model,
            max_input_chars=settings.llm_max_input_chars,
        )
    return _llm


class _StripHTML(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._parts: list[str] = []

    def handle_data(self, data: str) -> None:
        s = data.strip()
        if s:
            self._parts.append(s)

    @property
    def text(self) -> str:
        return " ".join(self._parts)


class ExtractorModule:
    name = "extractor"

    async def process(self, document: Document, context: dict) -> StageResult:
        t = time.monotonic()
        route = context.get("router", {}).get("route", "generic")
        doc_type = context.get("classifier", {}).get("type", "unknown")

        raw_text, structured = self._extract(document, route)
        fields = self._pattern_fields(raw_text) if raw_text else {}
        fields.update(structured)

        if settings.llm_enabled and (
            doc_type in _LLM_ENRICHED_TYPES or route == "image"
        ):
            fields = await self._llm_enrich(document, route, doc_type, raw_text, fields)

        return StageResult(
            module=self.name,
            status="success",
            data={
                "raw_text": raw_text[:_MAX_TEXT],
                "fields": fields,
                "metadata": {
                    "size_bytes": len(document.content),
                    "filename": document.filename,
                    "mimetype": document.mimetype,
                },
            },
            duration_ms=(time.monotonic() - t) * 1000,
        )

    async def health_check(self) -> bool:
        return True

    # ------------------------------------------------------------------
    # Route-based extraction
    # ------------------------------------------------------------------

    def _extract(self, document: Document, route: str) -> tuple[str, dict]:
        if route == "json":
            return self._from_json(document.content)
        if route == "spreadsheet" and "csv" in document.mimetype:
            return "", self._from_csv(document.content)
        if route == "html":
            return self._from_html(document.content), {}
        return self._from_text(document.content), {}

    def _from_json(self, content: bytes) -> tuple[str, dict]:
        try:
            data = json.loads(content)
            return "", {"json_data": data}
        except Exception:
            return content.decode("utf-8", errors="ignore"), {}

    def _from_csv(self, content: bytes) -> dict:
        try:
            text = content.decode("utf-8", errors="ignore")
            reader = csv.DictReader(io.StringIO(text))
            rows = list(reader)
            return {
                "columns": list(reader.fieldnames or []),
                "row_count": len(rows),
                "sample_rows": rows[:5],
            }
        except Exception:
            return {}

    def _from_html(self, content: bytes) -> str:
        parser = _StripHTML()
        parser.feed(content.decode("utf-8", errors="ignore"))
        return parser.text

    def _from_text(self, content: bytes) -> str:
        return content.decode("utf-8", errors="ignore")

    def _pattern_fields(self, text: str) -> dict:
        result: dict = {}
        for name, pattern in FIELD_PATTERNS.items():
            matches = pattern.findall(text)
            if matches:
                seen: set = set()
                result[name] = [m for m in matches if not (m in seen or seen.add(m))]
        return result

    # ------------------------------------------------------------------
    # LLM enrichment
    # ------------------------------------------------------------------

    async def _llm_enrich(
        self,
        document: Document,
        route: str,
        doc_type: str,
        raw_text: str,
        existing_fields: dict,
    ) -> dict:
        llm = _get_llm()
        if not llm.available:
            return existing_fields

        image_bytes: bytes | None = None
        image_media_type = "image/jpeg"
        if route == "image" and settings.llm_vision_enabled:
            image_bytes = document.content
            if document.mimetype:
                image_media_type = document.mimetype

        llm_fields = await llm.extract(
            text=raw_text,
            doc_type=doc_type,
            image_bytes=image_bytes,
            image_media_type=image_media_type,
        )
        if not llm_fields:
            return existing_fields

        merged = dict(existing_fields)
        for key, llm_values in llm_fields.items():
            if key not in merged:
                merged[key] = llm_values
            else:
                # Merge lists, deduplicating while preserving order
                existing = merged[key]
                if isinstance(existing, list) and isinstance(llm_values, list):
                    seen: set = set(map(str, existing))
                    extras = [v for v in llm_values if str(v) not in seen]
                    merged[key] = existing + extras
        return merged
