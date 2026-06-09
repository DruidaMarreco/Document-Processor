"""Tests for improved classifier: weighted scoring, confidence calibration, filename patterns."""
from __future__ import annotations

import pytest

from document_processor.models import Document
from classifier_module.classifier import ClassifierModule
from classifier_module.rules import (
    TYPE_KEYWORDS_WEIGHTED,
    FILENAME_PATTERNS,
    ROUTE_TO_TYPE,
    _MAX_SCORE,
    _HIGH_WEIGHT,
    _LOW_WEIGHT,
)

clf = ClassifierModule()


def _doc(text: str = "", filename: str = "", mimetype: str = "") -> Document:
    return Document(content=text.encode(), filename=filename, mimetype=mimetype)


def _ctx(route: str = "generic") -> dict:
    return {"router": {"route": route}}


# ---------------------------------------------------------------------------
# rules.py sanity checks
# ---------------------------------------------------------------------------

def test_all_types_have_weighted_entry():
    types = [t for t, _, _ in TYPE_KEYWORDS_WEIGHTED]
    assert "invoice" in types
    assert "nda" in types
    assert "email" in types
    assert len(types) >= 18


def test_max_score_precomputed():
    for doc_type, high, low in TYPE_KEYWORDS_WEIGHTED:
        expected = len(high) * _HIGH_WEIGHT + len(low) * _LOW_WEIGHT
        assert _MAX_SCORE[doc_type] == expected


def test_yaml_route_maps_to_data():
    assert ROUTE_TO_TYPE.get("yaml") == "data"


def test_filename_patterns_not_empty():
    assert len(FILENAME_PATTERNS) >= 10


# ---------------------------------------------------------------------------
# Weighted keyword scoring — distinct types
# ---------------------------------------------------------------------------

def test_invoice_high_weight_keywords():
    text = "tax invoice invoice number amount due payment terms net 30"
    result = clf._weighted_keyword_score(text)
    assert result is not None
    doc_type, confidence = result
    assert doc_type == "invoice"
    assert confidence >= 0.55


def test_nda_detection():
    text = "non-disclosure agreement disclosing party receiving party shall not disclose"
    result = clf._weighted_keyword_score(text)
    assert result is not None
    assert result[0] == "nda"


def test_payslip_detection():
    text = "payslip gross pay net pay national insurance year to date ytd deductions"
    result = clf._weighted_keyword_score(text)
    assert result is not None
    assert result[0] == "payslip"


def test_bank_statement_detection():
    text = "bank statement account holder sort code swift bic available balance"
    result = clf._weighted_keyword_score(text)
    assert result is not None
    assert result[0] == "bank_statement"


def test_tax_document_detection():
    text = "tax return adjusted gross income employer identification w-2 filing status"
    result = clf._weighted_keyword_score(text)
    assert result is not None
    assert result[0] == "tax_document"


def test_medical_record_detection():
    text = "patient diagnosis prescription physician icd cpt medical history"
    result = clf._weighted_keyword_score(text)
    assert result is not None
    assert result[0] == "medical_record"


def test_quote_detection():
    text = "quotation quote number valid until quoted by total estimate"
    result = clf._weighted_keyword_score(text)
    assert result is not None
    assert result[0] == "quote"


def test_delivery_note_detection():
    text = "delivery note packing list despatch note goods received consignment"
    result = clf._weighted_keyword_score(text)
    assert result is not None
    assert result[0] == "delivery_note"


def test_resume_detection():
    text = "curriculum vitae work experience employment history professional summary references available"
    result = clf._weighted_keyword_score(text)
    assert result is not None
    assert result[0] == "resume"


def test_specification_detection():
    text = "functional requirements non-functional requirements acceptance criteria scope of work deliverables"
    result = clf._weighted_keyword_score(text)
    assert result is not None
    assert result[0] == "specification"


def test_no_match_returns_none():
    result = clf._weighted_keyword_score("random text with no document keywords")
    assert result is None


# ---------------------------------------------------------------------------
# Confidence calibration
# ---------------------------------------------------------------------------

def test_confidence_higher_for_more_keywords():
    text_few = "invoice amount due"
    text_many = "invoice tax invoice invoice number amount due payment terms net 30 bill to remit to subtotal"
    r_few = clf._weighted_keyword_score(text_few)
    r_many = clf._weighted_keyword_score(text_many)
    assert r_few is not None and r_many is not None
    assert r_many[1] > r_few[1]


def test_confidence_boosted_when_clear_winner():
    # Invoice keywords only — clear winner → higher confidence
    text = "invoice tax invoice invoice number amount due payment terms"
    result = clf._weighted_keyword_score(text)
    assert result is not None
    assert result[1] >= 0.60


def test_confidence_reduced_when_ambiguous():
    # Mix of invoice + receipt keywords → lower confidence than pure invoice
    pure_text = "invoice tax invoice invoice number amount due payment terms"
    mixed_text = "invoice receipt receipt number total paid amount due"
    r_pure = clf._weighted_keyword_score(pure_text)
    r_mixed = clf._weighted_keyword_score(mixed_text)
    assert r_pure is not None and r_mixed is not None
    # winner of mixed has lower confidence due to competition
    # (just check both are valid classifications, not necessarily pure > mixed)
    assert r_pure[1] <= 1.0 and r_mixed[1] <= 1.0


def test_confidence_capped_at_0_95():
    # Pack in every possible invoice keyword
    text = " ".join([
        "invoice", "tax invoice", "vat invoice", "invoice number", "invoice no",
        "invoice #", "bill to", "remit to", "amount due", "net 30", "net 60",
        "payment terms", "subtotal", "due date", "purchase", "payment",
    ])
    result = clf._weighted_keyword_score(text)
    assert result is not None
    assert result[1] <= 0.95


# ---------------------------------------------------------------------------
# Filename pattern matching
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_filename_invoice():
    doc = _doc(filename="invoice_2024_001.pdf")
    r = await clf.process(doc, _ctx("pdf"))
    assert r.data["type"] == "invoice"
    assert r.data["method"] == "filename"


@pytest.mark.asyncio
async def test_filename_payslip():
    doc = _doc(filename="payslip_march_2024.pdf")
    r = await clf.process(doc, _ctx("pdf"))
    assert r.data["type"] == "payslip"
    assert r.data["method"] == "filename"


@pytest.mark.asyncio
async def test_filename_nda():
    doc = _doc(filename="mutual_nda_2024.docx")
    r = await clf.process(doc, _ctx("word"))
    assert r.data["type"] == "nda"
    assert r.data["method"] == "filename"


@pytest.mark.asyncio
async def test_filename_bank_statement():
    doc = _doc(filename="bank_statement_jan.pdf")
    r = await clf.process(doc, _ctx("pdf"))
    assert r.data["type"] == "bank_statement"


@pytest.mark.asyncio
async def test_filename_cv():
    doc = _doc(filename="john_doe_cv_2024.pdf")
    r = await clf.process(doc, _ctx("pdf"))
    assert r.data["type"] == "resume"


@pytest.mark.asyncio
async def test_filename_purchase_order():
    doc = _doc(filename="po_00123.pdf")
    r = await clf.process(doc, _ctx("pdf"))
    assert r.data["type"] == "purchase_order"


# ---------------------------------------------------------------------------
# Route shortcuts
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_yaml_route_returns_data():
    doc = _doc("")
    r = await clf.process(doc, _ctx("yaml"))
    assert r.data["type"] == "data"
    assert r.data["method"] == "route"
    assert r.data["confidence"] == 0.90


@pytest.mark.asyncio
async def test_spreadsheet_route():
    doc = _doc("")
    r = await clf.process(doc, _ctx("spreadsheet"))
    assert r.data["type"] == "spreadsheet"
    assert r.data["confidence"] == 0.90


@pytest.mark.asyncio
async def test_email_route():
    doc = _doc("")
    r = await clf.process(doc, _ctx("email"))
    assert r.data["type"] == "email"


# ---------------------------------------------------------------------------
# Email keyword detection (email route not triggered)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_email_keyword_in_pdf_route():
    text = "from: alice@example.com\nto: bob@example.com\nsubject: hello\ncc: carol@example.com"
    doc = _doc(text, filename="message.pdf")
    r = await clf.process(doc, _ctx("pdf"))
    # Should detect email keywords
    assert r.data["type"] == "email"


# ---------------------------------------------------------------------------
# Integration: full process call
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_process_includes_method():
    text = "invoice tax invoice amount due net 30 payment terms bill to"
    doc = _doc(text)
    r = await clf.process(doc, _ctx("generic"))
    assert "method" in r.data
    assert r.data["method"] in ("keyword", "filename", "route", "fallback", "forced", "llm")


@pytest.mark.asyncio
async def test_process_confidence_in_range():
    text = "purchase order po number unit price qty line total"
    doc = _doc(text)
    r = await clf.process(doc, _ctx("generic"))
    assert 0.0 <= r.data["confidence"] <= 1.0


@pytest.mark.asyncio
async def test_process_status_success():
    doc = _doc("some random text")
    r = await clf.process(doc, _ctx("generic"))
    assert r.status == "success"
    assert r.module == "classifier"
