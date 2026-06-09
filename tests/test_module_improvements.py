"""Tests for expanded classifier vocabulary and extractor patterns."""
from __future__ import annotations

import re

import pytest

from classifier_module.rules import ROUTE_TO_TYPE, TYPE_KEYWORDS
from extractor_module.patterns import FIELD_PATTERNS


# ---------------------------------------------------------------------------
# Classifier rule tests — new document types
# ---------------------------------------------------------------------------

_KEYWORD_MAP = {doc_type: kws for doc_type, kws in TYPE_KEYWORDS}


def _has_type(doc_type: str) -> bool:
    return doc_type in _KEYWORD_MAP


def test_nda_type_exists():
    assert _has_type("nda")


def test_nda_keywords_include_confidentiality():
    kws = _KEYWORD_MAP["nda"]
    assert any("confidential" in k for k in kws)


def test_bank_statement_type_exists():
    assert _has_type("bank_statement")


def test_bank_statement_keywords_include_iban():
    kws = _KEYWORD_MAP["bank_statement"]
    assert any("iban" in k.lower() for k in kws)


def test_tax_document_type_exists():
    assert _has_type("tax_document")


def test_tax_document_keywords_include_w2():
    kws = _KEYWORD_MAP["tax_document"]
    assert any("w-2" in k or "w2" in k for k in kws)


def test_payslip_type_exists():
    assert _has_type("payslip")


def test_payslip_keywords_include_gross_pay():
    kws = _KEYWORD_MAP["payslip"]
    assert any("gross pay" in k.lower() for k in kws)


def test_insurance_type_exists():
    assert _has_type("insurance")


def test_medical_record_type_exists():
    assert _has_type("medical_record")


def test_quote_type_exists():
    assert _has_type("quote")


def test_delivery_note_type_exists():
    assert _has_type("delivery_note")


def test_all_original_types_still_present():
    for t in ("invoice", "receipt", "purchase_order", "contract",
              "statement", "form", "report", "resume", "letter", "specification"):
        assert _has_type(t), f"Original type {t!r} is missing"


def test_invoice_keywords_expanded():
    kws = _KEYWORD_MAP["invoice"]
    assert len(kws) >= 8


def test_contract_keywords_include_governing_law():
    kws = _KEYWORD_MAP["contract"]
    assert any("governing law" in k.lower() for k in kws)


def test_resume_keywords_include_linkedin():
    kws = _KEYWORD_MAP["resume"]
    assert any("linkedin" in k.lower() for k in kws)


# ---------------------------------------------------------------------------
# Extractor pattern tests — new and improved patterns
# ---------------------------------------------------------------------------

def test_reference_numbers_pattern_exists():
    assert "reference_numbers" in FIELD_PATTERNS


def test_reference_numbers_matches_po():
    pat = FIELD_PATTERNS["reference_numbers"]
    assert pat.search("PO-12345")


def test_reference_numbers_matches_inv():
    pat = FIELD_PATTERNS["reference_numbers"]
    assert pat.search("INV #2024-001")


def test_iban_pattern_exists():
    assert "iban" in FIELD_PATTERNS


def test_iban_matches_gb():
    pat = FIELD_PATTERNS["iban"]
    assert pat.search("GB29NWBK60161331926819")


def test_iban_matches_de():
    pat = FIELD_PATTERNS["iban"]
    assert pat.search("DE89370400440532013000")


def test_postcodes_pattern_exists():
    assert "postcodes" in FIELD_PATTERNS


def test_postcodes_matches_us_zip():
    pat = FIELD_PATTERNS["postcodes"]
    assert pat.search("New York, NY 10001")


def test_postcodes_matches_us_zip_plus4():
    pat = FIELD_PATTERNS["postcodes"]
    assert pat.search("90210-1234")


def test_vat_numbers_pattern_exists():
    assert "vat_numbers" in FIELD_PATTERNS


def test_vat_numbers_matches_vat():
    pat = FIELD_PATTERNS["vat_numbers"]
    assert pat.search("VAT: GB123456789")


def test_vat_numbers_matches_ein():
    pat = FIELD_PATTERNS["vat_numbers"]
    assert pat.search("EIN 12-3456789")


def test_dates_matches_spelled_out_month():
    pat = FIELD_PATTERNS["dates"]
    assert pat.search("15 January 2024")


def test_amounts_matches_euro():
    pat = FIELD_PATTERNS["amounts"]
    assert pat.search("€1,234.56")


def test_amounts_matches_gbp():
    pat = FIELD_PATTERNS["amounts"]
    assert pat.search("£500.00")


def test_amounts_matches_eur_suffix():
    pat = FIELD_PATTERNS["amounts"]
    assert pat.search("500 EUR")


def test_emails_still_works():
    pat = FIELD_PATTERNS["emails"]
    assert pat.search("contact@example.com")


def test_phones_still_works():
    pat = FIELD_PATTERNS["phones"]
    assert pat.search("(555) 123-4567")


def test_urls_still_works():
    pat = FIELD_PATTERNS["urls"]
    assert pat.search("https://example.com/path")
