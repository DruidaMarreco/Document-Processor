from __future__ import annotations

import re
import time
from typing import Any

from document_processor.models import Document, StageResult

_EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$")
_DATE_ISO = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_URL_RE   = re.compile(r"^https?://[^\s]+$")

# required fields per document type (keys must exist and be non-empty)
REQUIRED: dict[str, list[str]] = {
    "invoice":        ["amounts", "dates"],
    "purchase_order": ["dates"],
    "email":          ["emails"],
    "contract":       ["dates"],
    "statement":      ["amounts", "dates"],
}


def _scalars(v: Any) -> list:
    return v if isinstance(v, list) else [v]


class ValidatorModule:
    name = "validator"

    async def process(self, document: Document, context: dict) -> StageResult:
        t = time.monotonic()
        doc_type = context.get("classifier", {}).get("type", "unknown")
        fields: dict = context.get("refiner", {}).get("fields", {})

        violations: list[str] = []
        warnings: list[str] = []

        # Required field checks
        for field in REQUIRED.get(doc_type, []):
            if not fields.get(field):
                violations.append(f"'{field}' required for {doc_type} but not found")

        # Per-value format checks
        for key, value in fields.items():
            for v in _scalars(value):
                if v is None:
                    continue
                if "email" in key and isinstance(v, str):
                    if not _EMAIL_RE.match(v):
                        warnings.append(f"Unexpected email format: {v!r}")
                if "date" in key and isinstance(v, str):
                    if not _DATE_ISO.match(v):
                        warnings.append(f"Date not normalised to ISO: {v!r}")
                if "url" in key and isinstance(v, str):
                    if not _URL_RE.match(v):
                        warnings.append(f"Suspicious URL: {v!r}")
                if any(t in key for t in ("amount", "price", "total", "balance")):
                    if isinstance(v, (int, float)) and v < 0:
                        violations.append(f"Negative value in '{key}': {v}")

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
