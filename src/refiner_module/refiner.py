from __future__ import annotations

import re
import time
from datetime import datetime
from typing import Any

from document_processor.models import Document, StageResult

_DATE_FORMATS = [
    # ISO
    "%Y-%m-%d",
    # US and EU slash/dash
    "%m/%d/%Y", "%d/%m/%Y",
    "%m-%d-%Y", "%d-%m-%Y",
    "%m/%d/%y", "%d/%m/%y",
    # Dot notation (31.01.2024)
    "%d.%m.%Y", "%d.%m.%y",
    # Long month names
    "%B %d, %Y", "%b %d, %Y",
    "%d %B %Y",  "%d %b %Y",
    "%B %dst, %Y", "%B %dnd, %Y", "%B %drd, %Y", "%B %dth, %Y",
]

# Strip everything that is not a digit, dot, or minus sign
_AMOUNT_STRIP = re.compile(r"[^\d.\-]")
# Currency prefixes (symbols only)
_CURRENCY_PREFIX = re.compile(r"^[$€£¥₹]")
# K/M/B multiplier suffix (e.g. "$1.5k", "2M")
_KMB_SUFFIX = re.compile(r"([\d.]+)\s*([KkMmBb])$")
_ORDINAL = re.compile(r"\b(\d{1,2})(?:st|nd|rd|th)\b", re.IGNORECASE)


def _try_parse_date(s: str) -> str | None:
    s = s.strip()
    # Strip ISO time component before trying date-only formats
    s_date_only = s.split("T")[0].split(" ")[0] if ("T" in s or len(s) > 10) else s
    for src in (s, s_date_only):
        for fmt in _DATE_FORMATS:
            try:
                return datetime.strptime(src, fmt).date().isoformat()
            except ValueError:
                pass
    # Handle ordinal day numbers: "15th January 2024" → "15 January 2024"
    normalized = _ORDINAL.sub(r"\1", s)
    if normalized != s:
        return _try_parse_date(normalized)
    return None


def _try_parse_amount(s: str) -> float | None:
    s = s.strip()
    # Remove currency prefix symbol
    s = _CURRENCY_PREFIX.sub("", s).strip()
    # Handle K/M/B multipliers
    m = _KMB_SUFFIX.match(s)
    if m:
        base = float(m.group(1))
        mult = {"k": 1_000, "m": 1_000_000, "b": 1_000_000_000}[m.group(2).lower()]
        return base * mult
    # Strip non-numeric characters (but keep dots and minus)
    cleaned = _AMOUNT_STRIP.sub("", s.replace(",", ""))
    # Handle multiple dots (keep only last as decimal separator)
    if cleaned.count(".") > 1:
        parts = cleaned.split(".")
        cleaned = "".join(parts[:-1]) + "." + parts[-1]
    try:
        return float(cleaned) if cleaned and cleaned != "-" else None
    except ValueError:
        return None


def _normalize_str(value: str) -> str:
    """Collapse internal whitespace and strip."""
    return " ".join(value.split())


def _dedup(lst: list) -> list:
    """Deduplicate preserving order; case-insensitive for strings."""
    seen: set = set()
    out = []
    for item in lst:
        key = str(item).lower() if isinstance(item, str) else str(item)
        if key not in seen:
            seen.add(key)
            out.append(item)
    return out


def _normalize(key: str, value: Any) -> Any:
    if isinstance(value, list):
        refined = [_normalize(key, v) for v in value]
        # Remove None and empty strings from lists
        refined = [v for v in refined if v is not None and v != ""]
        return _dedup(refined)
    if value is None:
        return value
    if not isinstance(value, str):
        return value

    value = _normalize_str(value)
    if not value:
        return value

    if "date" in key:
        return _try_parse_date(value) or value

    if any(t in key for t in ("amount", "price", "total", "due", "balance", "cost", "pay", "fee")):
        parsed = _try_parse_amount(value)
        return parsed if parsed is not None else value

    return value


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
