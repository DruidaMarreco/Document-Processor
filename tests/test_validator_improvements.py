"""Tests for improved validator: new doc types, IBAN, date plausibility, amount sanity."""
from __future__ import annotations

import pytest

from document_processor.models import Document
from validator_module.validator import ValidatorModule, _validate_iban

val = ValidatorModule()
_doc = Document(content=b"")


def _ctx(doc_type: str, fields: dict) -> dict:
    return {"classifier": {"type": doc_type}, "refiner": {"fields": fields}}


# ---------------------------------------------------------------------------
# New required-field coverage
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_receipt_requires_amounts():
    r = await val.process(_doc, _ctx("receipt", {}))
    assert r.data["valid"] is False
    assert any("amounts" in v for v in r.data["violations"])


@pytest.mark.asyncio
async def test_receipt_valid_with_amounts():
    r = await val.process(_doc, _ctx("receipt", {"amounts": [25.99]}))
    assert r.data["valid"] is True


@pytest.mark.asyncio
async def test_nda_requires_dates():
    r = await val.process(_doc, _ctx("nda", {}))
    assert r.data["valid"] is False
    assert any("dates" in v for v in r.data["violations"])


@pytest.mark.asyncio
async def test_bank_statement_requires_amounts_and_dates():
    r = await val.process(_doc, _ctx("bank_statement", {}))
    assert r.data["valid"] is False
    assert len(r.data["violations"]) >= 2


@pytest.mark.asyncio
async def test_payslip_requires_amounts_and_dates():
    r = await val.process(_doc, _ctx("payslip", {}))
    assert r.data["valid"] is False


@pytest.mark.asyncio
async def test_insurance_requires_dates():
    r = await val.process(_doc, _ctx("insurance", {}))
    assert r.data["valid"] is False


@pytest.mark.asyncio
async def test_quote_requires_amounts():
    r = await val.process(_doc, _ctx("quote", {}))
    assert r.data["valid"] is False
    assert any("amounts" in v for v in r.data["violations"])


@pytest.mark.asyncio
async def test_tax_document_requires_dates():
    r = await val.process(_doc, _ctx("tax_document", {}))
    assert r.data["valid"] is False


@pytest.mark.asyncio
async def test_delivery_note_requires_dates():
    r = await val.process(_doc, _ctx("delivery_note", {}))
    assert r.data["valid"] is False


# ---------------------------------------------------------------------------
# IBAN validation
# ---------------------------------------------------------------------------

def test_validate_iban_valid_gb():
    assert _validate_iban("GB29NWBK60161331926819") is True


def test_validate_iban_valid_de():
    assert _validate_iban("DE89370400440532013000") is True


def test_validate_iban_invalid_checksum():
    assert _validate_iban("GB00NWBK60161331926819") is False


def test_validate_iban_too_short():
    assert _validate_iban("GB29NW") is False


@pytest.mark.asyncio
async def test_valid_iban_no_warning():
    r = await val.process(_doc, _ctx("bank_statement", {
        "amounts": [100.0], "dates": ["2024-01-01"],
        "iban": ["GB29NWBK60161331926819"],
    }))
    iban_warnings = [w for w in r.data["warnings"] if "IBAN" in w]
    assert iban_warnings == []


@pytest.mark.asyncio
async def test_invalid_iban_warning():
    r = await val.process(_doc, _ctx("unknown", {
        "iban": ["GB00INVALID123456789"],
    }))
    assert any("IBAN" in w for w in r.data["warnings"])


# ---------------------------------------------------------------------------
# Date plausibility
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_ancient_date_warning():
    r = await val.process(_doc, _ctx("unknown", {"dates": ["1800-01-01"]}))
    assert any("implausibly old" in w for w in r.data["warnings"])


@pytest.mark.asyncio
async def test_future_date_warning():
    r = await val.process(_doc, _ctx("unknown", {"dates": ["2099-12-31"]}))
    assert any("future" in w for w in r.data["warnings"])


@pytest.mark.asyncio
async def test_normal_date_no_plausibility_warning():
    r = await val.process(_doc, _ctx("unknown", {"dates": ["2024-06-15"]}))
    plausibility_warnings = [w for w in r.data["warnings"] if "implausibly" in w or "future" in w]
    assert plausibility_warnings == []


# ---------------------------------------------------------------------------
# Amount sanity check
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_huge_amount_warning():
    r = await val.process(_doc, _ctx("invoice", {
        "amounts": [5_000_000_000],
        "dates": ["2024-01-01"],
    }))
    assert any("Unusually large" in w for w in r.data["warnings"])


@pytest.mark.asyncio
async def test_reasonable_amount_no_warning():
    r = await val.process(_doc, _ctx("invoice", {
        "amounts": [50_000.0],
        "dates": ["2024-01-01"],
    }))
    amount_warnings = [w for w in r.data["warnings"] if "large" in w.lower()]
    assert amount_warnings == []


# ---------------------------------------------------------------------------
# Phone format
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_phone_valid_no_warning():
    r = await val.process(_doc, _ctx("unknown", {"phone": ["+1-555-123-4567"]}))
    phone_warnings = [w for w in r.data["warnings"] if "phone" in w.lower()]
    assert phone_warnings == []


@pytest.mark.asyncio
async def test_phone_invalid_warning():
    r = await val.process(_doc, _ctx("unknown", {"phone": ["not-a-phone!!!!"]}))
    assert any("phone" in w.lower() for w in r.data["warnings"])
