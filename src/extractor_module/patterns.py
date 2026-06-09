from __future__ import annotations

import re

FIELD_PATTERNS: dict[str, re.Pattern] = {
    "dates": re.compile(
        r"\b(\d{4}-\d{2}-\d{2}"                   # ISO: 2024-01-31
        r"|\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}"      # US/EU: 01/31/2024 or 31-01-24
        r"|\d{1,2}\.\d{1,2}\.\d{2,4}"             # dot notation: 31.01.2024
        r"|\d{1,2}\s+(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?"
        r"|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?"
        r"|Nov(?:ember)?|Dec(?:ember)?)\s+\d{4}"  # 31 January 2024 / 31 Jan 2024
        r"|(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?"
        r"|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?"
        r"|Nov(?:ember)?|Dec(?:ember)?)\s+\d{1,2},?\s+\d{4}"  # January 31, 2024
        r")\b",
        re.IGNORECASE,
    ),
    "amounts": re.compile(
        r"(?:[$€£¥₹₩₫₴]\s*[\d,]+(?:\.\d{1,2})?"          # prefix symbol
        r"|[\d,]+(?:\.\d{1,2})?\s*(?:USD|EUR|GBP|CAD|AUD|NZD|JPY|CHF|INR|CNY|HKD|SGD|SEK|NOK|DKK))",
        re.IGNORECASE,
    ),
    "emails": re.compile(
        r"\b[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}\b"
    ),
    "phones": re.compile(
        r"\b(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b"   # North American
        r"|\+\d{1,3}[-.\s]\d{2,4}[-.\s]\d{3,4}[-.\s]?\d{3,4}"        # international +XX XX XXXX XXXX
        r"|\b0\d{2,4}[-.\s]?\d{3,4}[-.\s]?\d{3,4}\b",                 # local UK/EU style
        re.VERBOSE,
    ),
    "urls": re.compile(r"https?://[^\s\"'<>)\]]+"),
    "reference_numbers": re.compile(
        r"\b(?:PO|INV|REF|ORDER|DOC|CASE|TICKET|TKT|CONTRACT|QUOTE|QT|EST|DN|PN|SN|RMA)"
        r"(?:[-#\s]{0,2})[\w][\w\-]{2,19}\b",
        re.IGNORECASE,
    ),
    "iban": re.compile(
        r"\b[A-Z]{2}\d{2}[A-Z0-9]{4}\d{7}(?:[A-Z0-9]{0,16})\b"
    ),
    "swift_bic": re.compile(
        r"\b[A-Z]{4}[A-Z]{2}[A-Z0-9]{2}(?:[A-Z0-9]{3})?\b"
    ),
    "sort_codes": re.compile(
        r"\b\d{2}-\d{2}-\d{2}\b"                  # UK sort code: NN-NN-NN
    ),
    "postcodes": re.compile(
        r"\b\d{5}(?:-\d{4})?\b"                   # US ZIP / ZIP+4
        r"|(?<!\w)[A-Z]{1,2}\d[A-Z\d]?\s?\d[A-Z]{2}(?!\w)",  # UK postcode
        re.IGNORECASE,
    ),
    "vat_numbers": re.compile(
        r"\b(?:VAT|GST|ABN|EIN|TIN|NIP|CRN|SIRET|TVA)[-:\s]{0,2}[\w][\w\-]{4,14}\b",
        re.IGNORECASE,
    ),
    "company_registration": re.compile(
        r"\b(?:company\s+(?:no|number|reg)|registration\s+(?:no|number)|reg\s+no)"
        r"\.?\s*:?\s*([A-Z0-9]{6,12})\b",
        re.IGNORECASE,
    ),
}
