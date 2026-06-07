import pytest

from document_processor.models import Document
from refiner_module import RefinerModule

ref = RefinerModule()
_doc = Document(content=b"")


def _ctx(fields: dict) -> dict:
    return {"extractor": {"fields": fields, "metadata": {"size_bytes": 0}}}


@pytest.mark.asyncio
async def test_normalises_iso_date():
    r = await ref.process(_doc, _ctx({"dates": ["01/15/2026"]}))
    assert r.data["fields"]["dates"] == ["2026-01-15"]


@pytest.mark.asyncio
async def test_normalises_dmy_date():
    r = await ref.process(_doc, _ctx({"dates": ["15/01/2026"]}))
    # ambiguous but should not crash — returns something
    assert r.data["fields"]["dates"]


@pytest.mark.asyncio
async def test_normalises_already_iso_date():
    r = await ref.process(_doc, _ctx({"dates": ["2026-06-07"]}))
    assert r.data["fields"]["dates"] == ["2026-06-07"]


@pytest.mark.asyncio
async def test_parses_amount_currency_symbol():
    r = await ref.process(_doc, _ctx({"amounts": ["$1,250.00"]}))
    assert r.data["fields"]["amounts"] == [1250.0]


@pytest.mark.asyncio
async def test_parses_amount_no_symbol():
    r = await ref.process(_doc, _ctx({"amounts": ["99.99"]}))
    assert r.data["fields"]["amounts"] == [99.99]


@pytest.mark.asyncio
async def test_deduplicates_list_values():
    r = await ref.process(_doc, _ctx({"emails": ["a@b.com", "a@b.com", "c@d.com"]}))
    assert r.data["fields"]["emails"] == ["a@b.com", "c@d.com"]


@pytest.mark.asyncio
async def test_passes_through_non_field_keys():
    ctx = {"extractor": {"fields": {}, "metadata": {"size_bytes": 42}, "row_count": 10}}
    r = await ref.process(_doc, ctx)
    assert r.data["row_count"] == 10
    assert r.data["metadata"]["size_bytes"] == 42


@pytest.mark.asyncio
async def test_empty_fields_ok():
    r = await ref.process(_doc, _ctx({}))
    assert r.data["fields"] == {}
    assert r.status == "success"
