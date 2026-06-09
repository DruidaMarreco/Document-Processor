from __future__ import annotations

import re

FIELD_PATTERNS: dict[str, re.Pattern] = {
    "dates": re.compile(
        r"\b(\d{4}-\d{2}-\d{2}"           # ISO: 2024-01-31
        r"|\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}"  # US/EU: 01/31/2024 or 31-01-24
        r"|\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*"
        r"\s+\d{4})\b",                    # 31 January 2024
        re.IGNORECASE,
    ),
    "amounts": re.compile(
        r"(?:[$€£¥₹]\s*[\d,]+(?:\.\d{1,2})?"    # prefix symbol: $1,234.56
        r"|[\d,]+(?:\.\d{1,2})?\s*(?:USD|EUR|GBP|CAD|AUD|JPY|CHF|INR))",
        re.IGNORECASE,
    ),
    "emails": re.compile(
        r"\b[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}\b"
    ),
    "phones": re.compile(
        r"\b(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b"       # North American
        r"|\+\d{1,3}[-.\s]\d{2,4}[-.\s]\d{3,4}[-.\s]\d{3,4}",            # international +XX XX XXXX XXXX
        re.VERBOSE,
    ),
    "urls": re.compile(r"https?://[^\s\"'<>]+"),
    "reference_numbers": re.compile(
        r"\b(?:PO|INV|REF|ORDER|DOC|CASE|TICKET|TKT|CONTRACT|QUOTE|QT|EST)"
        r"(?:[-#\s]{0,2})[\w][\w\-]{2,19}\b",
        re.IGNORECASE,
    ),
    "iban": re.compile(
        r"\b[A-Z]{2}\d{2}[A-Z0-9]{4}\d{7}(?:[A-Z0-9]{0,16})\b"
    ),
    "postcodes": re.compile(
        r"\b\d{5}(?:-\d{4})?\b"                  # US ZIP / ZIP+4
        r"|[A-Z]{1,2}\d[A-Z\d]?\s?\d[A-Z]{2}",  # UK postcode
        re.IGNORECASE,
    ),
    "vat_numbers": re.compile(
        r"\b(?:VAT|GST|ABN|EIN|TIN|NIP)[-:\s]{0,2}[\w][\w\-]{4,14}\b",
        re.IGNORECASE,
    ),
}
