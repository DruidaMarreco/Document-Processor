from __future__ import annotations

import re
import time
from datetime import datetime
from typing import Any

from document_processor.models import Document, StageResult

_DATE_FORMATS = [
    "%Y-%m-%d",
    "%m/%d/%Y", "%d/%m/%Y",
    "%m-%d-%Y", "%d-%m-%Y",
    "%m/%d/%y", "%d/%m/%y",
    "%B %d, %Y", "%b %d, %Y",
    "%d %B %Y", "%d %b %Y",
]

_AMOUNT_RE = re.compile(r"[^\d.]")


def _try_parse_date(s: str) -> str | None:
    s = s.strip()
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            pass
    return None


def _try_parse_amount(s: str) -> float | None:
    cleaned = _AMOUNT_RE.sub("", s.replace(",", ""))
    try:
        return float(cleaned) if cleaned else None
    except ValueError:
        return None


def _normalize(key: str, value: Any) -> Any:
    if isinstance(value, list):
        return _dedup([_normalize(key, v) for v in value])
    if not isinstance(value, str):
        return value

    value = value.strip()
    if not value:
        return value

    if "date" in key:
        return _try_parse_date(value) or value

    if any(t in key for t in ("amount", "price", "total", "due", "balance", "cost")):
        return _try_parse_amount(value) if _try_parse_amount(value) is not None else value

    return value


def _dedup(lst: list) -> list:
    seen: set = set()
    out = []
    for item in lst:
        key = str(item)
        if key not in seen:
            seen.add(key)
            out.append(item)
    return out


class RefinerModule:
    name = "refiner"

    async def process(self, document: Document, context: dict) -> StageResult:
        t = time.monotonic()
        raw_fields: dict = context.get("extractor", {}).get("fields", {})
        refined = {k: _normalize(k, v) for k, v in raw_fields.items()}

        # Pass through non-field structured data (csv columns, json_data, etc.)
        extras = {
            k: v for k, v in context.get("extractor", {}).items()
            if k not in ("fields", "raw_text")
        }

        return StageResult(
            module=self.name,
            status="success",
            data={"fields": refined, "field_count": len(refined), **extras},
            duration_ms=(time.monotonic() - t) * 1000,
        )

    async def health_check(self) -> bool:
        return True
