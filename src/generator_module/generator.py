from __future__ import annotations

import time
from datetime import datetime, timezone

from document_processor.models import Document, StageResult


_METHOD_BONUS = {"llm": 0.10, "route": 0.05, "keyword": 0.0, "filename": -0.05, "forced": 0.15, "fallback": -0.15}


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


def _summary(context: dict, doc_type: str) -> str:
    route      = context.get("router",     {}).get("route", "unknown")
    confidence = context.get("classifier", {}).get("confidence", 0.0)
    fields     = context.get("refiner",    {}).get("fields", {})
    valid      = context.get("validator",  {}).get("valid", True)
    violations = context.get("validator",  {}).get("violations", [])
    warnings   = context.get("validator",  {}).get("warnings", [])

    parts = [
        f"Type: {doc_type} (route={route}, confidence={confidence:.0%})",
        f"Fields extracted: {len(fields)} ({', '.join(list(fields)[:6])})" if fields else "No fields extracted",
    ]

    if not valid:
        parts.append(f"Validation FAILED — {len(violations)} violation(s): {'; '.join(violations[:2])}")
    elif warnings:
        parts.append(f"Valid with {len(warnings)} warning(s)")
    else:
        parts.append("Validation passed")

    return ". ".join(parts) + "."


class GeneratorModule:
    name = "generator"

    async def process(self, document: Document, context: dict) -> StageResult:
        t = time.monotonic()
        doc_type = context.get("classifier", {}).get("type", "unknown")

        output = {
            "document_type":    doc_type,
            "route":            context.get("router",     {}).get("route"),
            "confidence":       context.get("classifier", {}).get("confidence"),
            "fields":           context.get("refiner",    {}).get("fields", {}),
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
