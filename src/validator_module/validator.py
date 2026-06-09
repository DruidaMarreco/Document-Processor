from __future__ import annotations

import re
import time
from datetime import date, datetime
from typing import Any

from document_processor.models import Document, StageResult

_EMAIL_RE   = re.compile(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$")
_DATE_ISO   = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_URL_RE     = re.compile(r"^https?://[^\s]+$")
_IBAN_CHARS = re.compile(r"^[A-Z]{2}\d{2}[A-Z0-9]+$")
_PHONE_RE   = re.compile(r"^\+?[\d\s\-().]{7,20}$")

# required fields per document type (keys must exist and be non-empty)
REQUIRED: dict[str, list[str]] = {
    "invoice":        ["amounts", "dates"],
    "receipt":        ["amounts"],
    "purchase_order": ["dates"],
    "email":          ["emails"],
    "contract":       ["dates"],
    "nda":            ["dates"],
    "statement":      ["amounts", "dates"],
    "bank_statement": ["amounts", "dates"],
    "tax_document":   ["dates"],
    "payslip":        ["amounts", "dates"],
    "insurance":      ["dates"],
    "quote":          ["amounts"],
    "delivery_note":  ["dates"],
}

# Plausibility thresholds
_MAX_REASONABLE_AMOUNT = 1_000_000_000   # 1 billion
_MIN_YEAR = 1900
_MAX_YEAR_OFFSET = 20   # warn if > 20 years in future


def _scalars(v: Any) -> list:
    return v if isinstance(v, list) else [v]


def _validate_iban(iban: str) -> bool:
    """Validate IBAN checksum using mod-97 algorithm."""
    if not _IBAN_CHARS.match(iban):
        return False
    rearranged = iban[4:] + iban[:4]
    numeric = "".join(
        str(ord(c) - 55) if c.isalpha() else c
        for c in rearranged
    )
    try:
        return int(numeric) % 97 == 1
    except ValueError:
        return False


def _parse_iso_date(s: str) -> date | None:
    if _DATE_ISO.match(s):
        try:
            return datetime.strptime(s, "%Y-%m-%d").date()
        except ValueError:
            pass
    return None


class ValidatorModule:
    name = "validator"

    async def process(self, document: Document, context: dict) -> StageResult:
        t = time.monotonic()
        doc_type = context.get("classifier", {}).get("type", "unknown")
        fields: dict = context.get("refiner", {}).get("fields", {})

        violations: list[str] = []
        warnings: list[str] = []
        today = date.today()

        # Required field checks
        for field in REQUIRED.get(doc_type, []):
            if not fields.get(field):
                violations.append(f"'{field}' required for {doc_type} but not found")

        # Per-value format checks
        for key, value in fields.items():
            for v in _scalars(value):
                if v is None:
                    continue

                # Email format
                if "email" in key and isinstance(v, str):
                    if not _EMAIL_RE.match(v):
                        warnings.append(f"Unexpected email format: {v!r}")

                # Date format and plausibility
                if "date" in key and isinstance(v, str):
                    if not _DATE_ISO.match(v):
                        warnings.append(f"Date not normalised to ISO: {v!r}")
                    else:
                        d = _parse_iso_date(v)
                        if d:
                            if d.year < _MIN_YEAR:
                                warnings.append(f"Date implausibly old: {v!r}")
                            elif d.year > today.year + _MAX_YEAR_OFFSET:
                                warnings.append(f"Date implausibly far in future: {v!r}")

                # URL format
                if "url" in key and isinstance(v, str):
                    if not _URL_RE.match(v):
                        warnings.append(f"Suspicious URL: {v!r}")

                # Amount checks
                if any(t in key for t in ("amount", "price", "total", "balance", "cost", "pay", "fee")):
                    if isinstance(v, (int, float)):
                        if v < 0:
                            violations.append(f"Negative value in '{key}': {v}")
                        elif v > _MAX_REASONABLE_AMOUNT:
                            warnings.append(
                                f"Unusually large value in '{key}': {v:,.2f} — verify this is correct"
                            )

                # IBAN validation
                if "iban" in key and isinstance(v, str):
                    clean = v.replace(" ", "").upper()
                    if len(clean) >= 15 and not _validate_iban(clean):
                        warnings.append(f"IBAN checksum invalid: {v!r}")

                # Phone format
                if "phone" in key and isinstance(v, str):
                    if not _PHONE_RE.match(v):
                        warnings.append(f"Unexpected phone format: {v!r}")

        return StageResult(
            module=self.name,
            status="success",
            data={
                "valid": len(violations) == 0,
                "violations": violations,
                "warnings": warnings,
            },
            duration_ms=(time.monotonic() - t) * 1000,
        )

    async def health_check(self) -> bool:
        return True
