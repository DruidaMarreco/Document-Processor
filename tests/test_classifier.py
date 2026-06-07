import pytest

from document_processor.models import Document
from classifier_module import ClassifierModule

clf = ClassifierModule()


def _ctx(route: str) -> dict:
    return {"router": {"route": route}}


def _doc(content: bytes = b"", filename: str | None = None) -> Document:
    return Document(content=content, filename=filename)


@pytest.mark.asyncio
async def test_image_route():
    r = await clf.process(_doc(), _ctx("image"))
    assert r.data["type"] == "image"
    assert r.data["method"] == "route"


@pytest.mark.asyncio
async def test_spreadsheet_route():
    r = await clf.process(_doc(), _ctx("spreadsheet"))
    assert r.data["type"] == "spreadsheet"


@pytest.mark.asyncio
async def test_email_route():
    r = await clf.process(_doc(), _ctx("email"))
    assert r.data["type"] == "email"


@pytest.mark.asyncio
async def test_keyword_invoice():
    content = b"Invoice No: 1234\nBill To: John\nAmount Due: $500\nDue Date: 2026-01-01"
    r = await clf.process(_doc(content), _ctx("pdf"))
    assert r.data["type"] == "invoice"
    assert r.data["method"] == "keyword"
    assert r.data["confidence"] > 0.5


@pytest.mark.asyncio
async def test_keyword_contract():
    content = b"This Agreement is entered into by the parties. Whereas the parties agree hereinafter."
    r = await clf.process(_doc(content), _ctx("word"))
    assert r.data["type"] == "contract"


@pytest.mark.asyncio
async def test_keyword_resume():
    content = b"Work Experience\nEducation\nSkills\nReferences available upon request"
    r = await clf.process(_doc(content), _ctx("pdf"))
    assert r.data["type"] == "resume"


@pytest.mark.asyncio
async def test_filename_hint():
    r = await clf.process(_doc(b"no keywords", filename="invoice_2026.pdf"), _ctx("pdf"))
    assert r.data["type"] == "invoice"
    assert r.data["method"] == "filename"


@pytest.mark.asyncio
async def test_unknown_fallback():
    r = await clf.process(_doc(b"\x00\x01\x02"), _ctx("generic"))
    assert r.data["type"] == "unknown"


@pytest.mark.asyncio
async def test_always_success():
    r = await clf.process(_doc(b""), _ctx("generic"))
    assert r.status == "success"
    assert r.module == "classifier"
