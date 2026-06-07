from __future__ import annotations

import re

FIELD_PATTERNS: dict[str, re.Pattern] = {
    "dates":   re.compile(
        r"\b(\d{4}-\d{2}-\d{2}|\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4})\b"
    ),
    "amounts": re.compile(
        r"(?:[$€£]\s*[\d,]+(?:\.\d{2})?|[\d,]+(?:\.\d{2})?\s*(?:USD|EUR|GBP))",
        re.IGNORECASE,
    ),
    "emails":  re.compile(
        r"\b[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}\b"
    ),
    "phones":  re.compile(
        r"\b(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b"
    ),
    "urls":    re.compile(r"https?://[^\s\"'>]+"),
}
