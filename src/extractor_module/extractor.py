from __future__ import annotations

import csv
import io
import json
import time
from html.parser import HTMLParser

from document_processor.models import Document, StageResult

from extractor_module.patterns import FIELD_PATTERNS

_MAX_TEXT = 8_000  # chars kept in context for downstream stages


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

        raw_text, structured = self._extract(document, route)
        fields = self._pattern_fields(raw_text) if raw_text else {}
        fields.update(structured)

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

    def _extract(self, document: Document, route: str) -> tuple[str, dict]:
        if route == "json":
            return self._from_json(document.content)
        if route == "spreadsheet" and "csv" in document.mimetype:
            return "", self._from_csv(document.content)
        if route == "html":
            return self._from_html(document.content), {}
        # pdf / word / text / generic — try UTF-8 decode
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
