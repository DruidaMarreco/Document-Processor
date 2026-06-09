"""Tests for improved generator: quality_score, key_facts, enriched summary."""
from __future__ import annotations

import pytest

from document_processor.models import Document
from generator_module.generator import (
    GeneratorModule,
    _quality_score,
    _key_facts,
    _field_confidence,
    _summary,
    _format_fact,
)

gen = GeneratorModule()
_doc = Document(content=b"")


def _ctx(
    doc_type="invoice",
    confidence=0.85,
    method="keyword",
    fields=None,
    valid=True,
    violations=None,
    warnings=None,
    route="pdf",
) -> dict:
    return {
        "router":     {"route": route},
        "classifier": {"type": doc_type, "confidence": confidence, "method": method},
        "extractor":  {"fields": fields or {}, "metadata": {}},
        "refiner":    {"fields": fields or {}},
        "validator":  {
            "valid": valid,
            "violations": violations or [],
            "warnings":   warnings or [],
        },
    }


# ---------------------------------------------------------------------------
# _quality_score
# ---------------------------------------------------------------------------

def test_quality_score_perfect():
    ctx = _ctx(confidence=1.0, method="llm", fields={f"f{i}": "v" for i in range(10)}, valid=True)
    score = _quality_score(ctx)
    assert score == 100


def test_quality_score_zero_confidence_no_fields():
    ctx = _ctx(confidence=0.0, fields={}, valid=True)
    assert _quality_score(ctx) == 20  # 0 + 0 + 20


def test_quality_score_with_violations():
    ctx = _ctx(confidence=0.8, fields={"amounts": [100], "dates": ["2024-01-01"]},
               valid=False, violations=["v1", "v2"])
    score = _quality_score(ctx)
    # 32 + min(30, 2*3) + max(0, 20 - 2*5) = 32 + 6 + 10 = 48
    assert score == 48


def test_quality_score_with_warnings():
    ctx = _ctx(confidence=0.5, fields={"amounts": [1]},
               valid=True, warnings=["w1", "w2", "w3"])
    score = _quality_score(ctx)
    # 20 + 3 + max(0, 20 - 0*5 - 3*2) = 20 + 3 + 14 = 37
    assert score == 37


def test_quality_score_llm_bonus():
    ctx = _ctx(confidence=0.5, method="llm", fields={"f": "v"}, valid=True)
    score_llm = _quality_score(ctx)
    ctx2 = _ctx(confidence=0.5, method="keyword", fields={"f": "v"}, valid=True)
    score_kw = _quality_score(ctx2)
    assert score_llm == score_kw + 10


def test_quality_score_capped_at_100():
    ctx = _ctx(confidence=1.0, method="llm",
               fields={f"f{i}": "v" for i in range(20)}, valid=True)
    assert _quality_score(ctx) == 100


def test_quality_score_min_zero():
    ctx = _ctx(confidence=0.0, fields={},
               valid=False, violations=["v"] * 10)
    assert _quality_score(ctx) == 0


# ---------------------------------------------------------------------------
# _key_facts
# ---------------------------------------------------------------------------

def test_key_facts_invoice():
    fields = {
        "amounts": [500.0, 200.0],
        "dates":   ["2024-01-01"],
        "parties": ["Acme Corp"],
        "reference_numbers": ["INV-001"],
    }
    facts = _key_facts("invoice", fields)
    assert "amounts" in facts
    assert "dates" in facts
    assert "parties" in facts
    assert "reference_numbers" in facts


def test_key_facts_single_value_list():
    fields = {"amounts": [99.99]}
    facts = _key_facts("invoice", fields)
    # Single-element list → scalar
    assert facts["amounts"] == 99.99


def test_key_facts_multi_value_list_capped():
    fields = {"amounts": [1.0, 2.0, 3.0, 4.0, 5.0]}
    facts = _key_facts("invoice", fields)
    # Capped at first 3
    assert facts["amounts"] == [1.0, 2.0, 3.0]


def test_key_facts_missing_fields():
    facts = _key_facts("invoice", {})
    assert facts == {}


def test_key_facts_unknown_doc_type():
    fields = {"amounts": [100]}
    facts = _key_facts("unknown_type", fields)
    assert facts == {}


def test_key_facts_receipt():
    fields = {"amounts": [25.99], "dates": ["2024-06-01"]}
    facts = _key_facts("receipt", fields)
    assert "amounts" in facts
    assert "dates" in facts


def test_key_facts_email():
    fields = {"emails": ["user@example.com"], "dates": ["2024-01-01"]}
    facts = _key_facts("email", fields)
    assert "emails" in facts
    assert "dates" in facts


def test_key_facts_nda():
    fields = {"parties": ["Company A", "Company B"], "dates": ["2024-01-01"]}
    facts = _key_facts("nda", fields)
    assert "parties" in facts
    assert "dates" in facts


# ---------------------------------------------------------------------------
# _format_fact
# ---------------------------------------------------------------------------

def test_format_fact_float():
    assert _format_fact("amount", 1234.56) == "1,234.56"


def test_format_fact_list():
    assert _format_fact("parties", ["Acme", "Beta"]) == "Acme, Beta"


def test_format_fact_string():
    assert _format_fact("date", "2024-01-01") == "2024-01-01"


# ---------------------------------------------------------------------------
# _field_confidence
# ---------------------------------------------------------------------------

def test_field_confidence_llm_bonus():
    ctx = _ctx(method="llm", confidence=0.8, fields={"amounts": [100], "dates": ["2024"]})
    scores = _field_confidence(ctx)
    for score in scores.values():
        assert score >= 0.1
        assert score <= 1.0


def test_field_confidence_violation_penalty():
    ctx = _ctx(confidence=0.9, fields={"amounts": [100]},
               valid=False, violations=["amounts: negative value"])
    scores = _field_confidence(ctx)
    # amounts field should have a penalty applied
    assert "amounts" in scores


def test_field_confidence_fallback_method():
    ctx = _ctx(method="fallback", confidence=0.3, fields={"dates": ["2024-01-01"]})
    scores = _field_confidence(ctx)
    assert "dates" in scores
    assert scores["dates"] <= 1.0


# ---------------------------------------------------------------------------
# _summary
# ---------------------------------------------------------------------------

def test_summary_contains_type_and_route():
    ctx = _ctx(doc_type="invoice", route="pdf", confidence=0.9)
    s = _summary(ctx, "invoice")
    assert "invoice" in s
    assert "pdf" in s


def test_summary_contains_key_facts():
    ctx = _ctx(
        doc_type="invoice",
        confidence=0.9,
        fields={"amounts": [500.0], "dates": ["2024-01-01"]},
    )
    s = _summary(ctx, "invoice")
    assert "Key:" in s


def test_summary_no_fields():
    ctx = _ctx(doc_type="invoice", confidence=0.5, fields={})
    s = _summary(ctx, "invoice")
    assert "No fields extracted" in s


def test_summary_validation_failed():
    ctx = _ctx(
        doc_type="invoice", confidence=0.7,
        fields={"amounts": [100]},
        valid=False, violations=["'dates' required"],
    )
    s = _summary(ctx, "invoice")
    assert "FAILED" in s


def test_summary_validation_warnings():
    ctx = _ctx(
        doc_type="invoice", confidence=0.7,
        fields={"amounts": [100], "dates": ["2024-01-01"]},
        valid=True, warnings=["Date not normalised"],
    )
    s = _summary(ctx, "invoice")
    assert "warning" in s.lower()


def test_summary_ends_with_period():
    ctx = _ctx()
    s = _summary(ctx, "invoice")
    assert s.endswith(".")


# ---------------------------------------------------------------------------
# GeneratorModule.process — integration
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_process_includes_quality_score():
    ctx = _ctx(confidence=0.85, method="keyword", fields={"amounts": [100]})
    r = await gen.process(_doc, ctx)
    assert "quality_score" in r.data["output"]
    assert isinstance(r.data["output"]["quality_score"], int)


@pytest.mark.asyncio
async def test_process_includes_key_facts():
    ctx = _ctx(
        doc_type="invoice", confidence=0.9,
        fields={"amounts": [100], "dates": ["2024-01-01"]},
    )
    r = await gen.process(_doc, ctx)
    assert "key_facts" in r.data["output"]


@pytest.mark.asyncio
async def test_process_includes_classification_method():
    ctx = _ctx(method="llm")
    r = await gen.process(_doc, ctx)
    assert r.data["output"]["classification_method"] == "llm"


@pytest.mark.asyncio
async def test_process_includes_field_confidence():
    ctx = _ctx(fields={"amounts": [100], "dates": ["2024-01-01"]})
    r = await gen.process(_doc, ctx)
    assert "field_confidence" in r.data["output"]
    assert isinstance(r.data["output"]["field_confidence"], dict)


@pytest.mark.asyncio
async def test_process_output_structure():
    ctx = _ctx()
    r = await gen.process(_doc, ctx)
    output = r.data["output"]
    for key in ("document_type", "route", "confidence", "quality_score",
                "fields", "key_facts", "field_confidence", "validation",
                "metadata", "processed_at", "classification_method"):
        assert key in output, f"Missing key: {key}"


@pytest.mark.asyncio
async def test_process_status_success():
    ctx = _ctx()
    r = await gen.process(_doc, ctx)
    assert r.status == "success"
    assert r.module == "generator"
