import pytest

from document_processor.models import Document
from validator_module import ValidatorModule

val = ValidatorModule()
_doc = Document(content=b"")


def _ctx(doc_type: str, fields: dict) -> dict:
    return {"classifier": {"type": doc_type}, "refiner": {"fields": fields}}


@pytest.mark.asyncio
async def test_valid_invoice_with_required_fields():
    r = await val.process(_doc, _ctx("invoice", {"amounts": [100.0], "dates": ["2026-01-01"]}))
    assert r.data["valid"] is True
    assert r.data["violations"] == []


@pytest.mark.asyncio
async def test_invoice_missing_amounts():
    r = await val.process(_doc, _ctx("invoice", {"dates": ["2026-01-01"]}))
    assert r.data["valid"] is False
    assert any("amounts" in v for v in r.data["violations"])


@pytest.mark.asyncio
async def test_email_doc_requires_emails():
    r = await val.process(_doc, _ctx("email", {}))
    assert r.data["valid"] is False
    assert any("emails" in v for v in r.data["violations"])


@pytest.mark.asyncio
async def test_bad_email_format_warning():
    r = await val.process(_doc, _ctx("unknown", {"emails": ["not-an-email"]}))
    assert r.data["valid"] is True          # warning, not violation
    assert any("email" in w.lower() for w in r.data["warnings"])


@pytest.mark.asyncio
async def test_unnormalised_date_warning():
    r = await val.process(_doc, _ctx("unknown", {"dates": ["01/15/2026"]}))
    assert any("ISO" in w for w in r.data["warnings"])


@pytest.mark.asyncio
async def test_negative_amount_violation():
    r = await val.process(_doc, _ctx("unknown", {"amounts": [-50.0]}))
    assert r.data["valid"] is False
    assert any("Negative" in v for v in r.data["violations"])


@pytest.mark.asyncio
async def test_unknown_type_no_required_fields():
    r = await val.process(_doc, _ctx("unknown", {}))
    assert r.data["valid"] is True          # no required fields for unknown type


@pytest.mark.asyncio
async def test_always_success_status():
    r = await val.process(_doc, _ctx("invoice", {}))
    assert r.status == "success"            # validator never errors, it reports violations
    assert r.module == "validator"
