import pytest

from document_processor.models import Document
from generator_module import GeneratorModule

gen = GeneratorModule()
_doc = Document(content=b"")


def _full_ctx(
    route="pdf", doc_type="invoice",
    confidence=0.85, fields=None,
    valid=True, violations=None, warnings=None,
) -> dict:
    return {
        "router":     {"route": route},
        "classifier": {"type": doc_type, "confidence": confidence},
        "extractor":  {"fields": fields or {}, "metadata": {"size_bytes": 100}},
        "refiner":    {"fields": fields or {}},
        "validator":  {"valid": valid, "violations": violations or [], "warnings": warnings or []},
    }


@pytest.mark.asyncio
async def test_output_contains_all_keys():
    r = await gen.process(_doc, _full_ctx())
    out = r.data["output"]
    assert out["document_type"] == "invoice"
    assert out["route"] == "pdf"
    assert out["confidence"] == 0.85
    assert "validation" in out
    assert "processed_at" in out


@pytest.mark.asyncio
async def test_summary_mentions_type_and_route():
    r = await gen.process(_doc, _full_ctx(doc_type="contract", route="word"))
    assert "contract" in r.data["summary"].lower()
    assert "word" in r.data["summary"].lower()


@pytest.mark.asyncio
async def test_summary_reports_validation_failure():
    r = await gen.process(_doc, _full_ctx(valid=False, violations=["Missing 'amounts'"]))
    assert "FAILED" in r.data["summary"] or "violation" in r.data["summary"].lower()


@pytest.mark.asyncio
async def test_fields_in_output():
    fields = {"amounts": [100.0], "dates": ["2026-01-01"]}
    r = await gen.process(_doc, _full_ctx(fields=fields))
    assert r.data["output"]["fields"] == fields


@pytest.mark.asyncio
async def test_format_is_json():
    r = await gen.process(_doc, _full_ctx())
    assert r.data["format"] == "json"


@pytest.mark.asyncio
async def test_empty_context_does_not_crash():
    r = await gen.process(_doc, {})
    assert r.status == "success"
    assert r.module == "generator"
