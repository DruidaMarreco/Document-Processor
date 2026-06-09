from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

from document_processor.models import Document, StageResult


_METHOD_BONUS = {
    "llm": 0.10, "route": 0.05, "keyword": 0.0,
    "filename": -0.05, "forced": 0.15, "fallback": -0.15,
}

# Fields to highlight per document type in the summary line
_KEY_FIELDS: dict[str, list[str]] = {
    "invoice":        ["amounts", "dates", "parties", "reference_numbers"],
    "receipt":        ["amounts", "dates"],
    "purchase_order": ["reference_numbers", "dates", "amounts"],
    "contract":       ["parties", "dates"],
    "nda":            ["parties", "dates"],
    "statement":      ["amounts", "dates"],
    "bank_statement": ["amounts", "dates", "iban"],
    "tax_document":   ["dates"],
    "payslip":        ["amounts", "dates"],
    "insurance":      ["dates", "reference_numbers"],
    "quote":          ["amounts", "reference_numbers"],
    "delivery_note":  ["reference_numbers", "dates"],
    "email":          ["emails", "dates"],
    "report":         ["dates"],
    "resume":         ["emails"],
    "letter":         ["dates", "emails"],
}


def _field_confidence(context: dict) -> dict[str, float]:
    """Derive a per-field confidence score from classifier + validation data."""
    base = float(context.get("classifier", {}).get("confidence") or 0.5)
    method = context.get("classifier", {}).get("method", "fallback")
    bonus = _METHOD_BONUS.get(method, 0.0)
    adjusted_base = min(1.0, max(0.1, base + bonus))

    violations: list[str] = context.get("validator", {}).get("violations", [])
    violated_fields: set[str] = set()
    for v in violations:
        word = v.split()[0].lower().rstrip(":,") if v else ""
        if word:
            violated_fields.add(word)

    fields: dict = context.get("refiner", {}).get("fields", {})
    scores: dict[str, float] = {}
    for field_name in fields:
        penalty = 0.3 if any(field_name in vf or vf in field_name for vf in violated_fields) else 0.0
        scores[field_name] = round(min(1.0, max(0.1, adjusted_base - penalty)), 3)
    return scores


def _quality_score(context: dict) -> int:
    """Compute a 0–100 quality score for this processing result.

    Components:
      - Classifier confidence (up to 40 pts)
      - Field richness: 3 pts per field, capped at 30 pts
      - Validation passed: 20 pts; deduct 5 per violation, 2 per warning (min 0)
      - Bonus 10 pts if LLM classification was used
    """
    confidence = float(context.get("classifier", {}).get("confidence") or 0.0)
    method = context.get("classifier", {}).get("method", "fallback")
    fields = context.get("refiner", {}).get("fields", {})
    valid = context.get("validator", {}).get("valid", True)
    violations = context.get("validator", {}).get("violations", [])
    warnings = context.get("validator", {}).get("warnings", [])

    score = int(confidence * 40)
    score += min(30, len(fields) * 3)
    validation_pts = max(0, 20 - len(violations) * 5 - len(warnings) * 2)
    score += validation_pts
    if method == "llm":
        score += 10

    return min(100, max(0, score))


def _key_facts(doc_type: str, fields: dict) -> dict[str, Any]:
    """Extract the most relevant facts for a given document type."""
    key_field_names = _KEY_FIELDS.get(doc_type, [])
    facts: dict[str, Any] = {}
    for field in key_field_names:
        val = fields.get(field)
        if val:
            # For lists, take the first value as the primary fact
            if isinstance(val, list):
                facts[field] = val[0] if len(val) == 1 else val[:3]
            else:
                facts[field] = val
    return facts


def _format_fact(key: str, val: Any) -> str:
    if isinstance(val, list):
        return ", ".join(str(v) for v in val[:2])
    if isinstance(val, float):
        return f"{val:,.2f}"
    return str(val)


def _summary(context: dict, doc_type: str) -> str:
    route      = context.get("router",     {}).get("route", "unknown")
    confidence = context.get("classifier", {}).get("confidence", 0.0)
    fields     = context.get("refiner",    {}).get("fields", {})
    valid      = context.get("validator",  {}).get("valid", True)
    violations = context.get("validator",  {}).get("violations", [])
    warnings   = context.get("validator",  {}).get("warnings", [])

    parts = [
        f"Type: {doc_type} (route={route}, confidence={confidence:.0%})",
    ]

    # Include top key facts in the summary
    facts = _key_facts(doc_type, fields)
    if facts:
        fact_str = "; ".join(f"{k}={_format_fact(k, v)}" for k, v in facts.items())
        parts.append(f"Key: {fact_str}")
    elif fields:
        parts.append(f"Fields extracted: {len(fields)} ({', '.join(list(fields)[:6])})")
    else:
        parts.append("No fields extracted")

    if not valid:
        parts.append(f"Validation FAILED — {len(violations)} violation(s): {'; '.join(violations[:2])}")
    elif warnings:
        parts.append(f"Valid with {len(warnings)} warning(s)")
    else:
        parts.append("Validation passed")

    return ". ".join(parts) + "."


def _stage_timings(context: dict) -> dict[str, float]:
    """Collect per-stage duration_ms from the context (populated by pipeline)."""
    return {
        stage: round(context.get(stage, {}).get("_duration_ms", 0.0), 2)
        for stage in ("router", "classifier", "extractor", "refiner", "validator")
        if context.get(stage, {}).get("_duration_ms") is not None
    }


class GeneratorModule:
    name = "generator"

    async def process(self, document: Document, context: dict) -> StageResult:
        t = time.monotonic()
        doc_type = context.get("classifier", {}).get("type", "unknown")
        fields = context.get("refiner", {}).get("fields", {})

        output = {
            "document_type":    doc_type,
            "route":            context.get("router",     {}).get("route"),
            "confidence":       context.get("classifier", {}).get("confidence"),
            "classification_method": context.get("classifier", {}).get("method"),
            "quality_score":    _quality_score(context),
            "fields":           fields,
            "key_facts":        _key_facts(doc_type, fields),
            "field_confidence": _field_confidence(context),
            "validation": {
                "passed":     context.get("validator", {}).get("valid", True),
                "violations": context.get("validator", {}).get("violations", []),
                "warnings":   context.get("validator", {}).get("warnings",   []),
            },
            "metadata":      context.get("extractor",  {}).get("metadata", {}),
            "processed_at":  datetime.now(timezone.utc).isoformat(),
        }

        return StageResult(
            module=self.name,
            status="success",
            data={
                "output":  output,
                "summary": _summary(context, doc_type),
                "format":  "json",
            },
            duration_ms=(time.monotonic() - t) * 1000,
        )

    async def health_check(self) -> bool:
        return True
