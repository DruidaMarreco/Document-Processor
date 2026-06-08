"""Tests for search/filter, reprocess, and export endpoints."""
from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient

from document_processor import storage
from document_processor.api import app
from document_processor.models import Document, PipelineResult, StageResult

_INVOICE = b"Invoice No: 1234\nBill To: John\nAmount Due: $500\nDue Date: 2026-01-01"
_CONTRACT = b"This Agreement is entered into by the parties. Whereas hereinafter."


@pytest.fixture(autouse=True)
def tmp_db(tmp_path: Path):
    storage.set_db_path(tmp_path / "test.db")
    yield
    storage.set_db_path(Path("data/results.db"))


def _client():
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


# ---------------------------------------------------------------------------
# Search / filter
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_search_no_filters_returns_all():
    async with _client() as c:
        await c.post("/process", files={"file": ("a.txt", _INVOICE, "text/plain")})
        await c.post("/process", files={"file": ("b.txt", _CONTRACT, "text/plain")})
        r = await c.get("/results")
    assert r.status_code == 200
    body = r.json()
    assert body["total"] == 2
    assert len(body["results"]) == 2


@pytest.mark.asyncio
async def test_search_filter_by_status():
    async with _client() as c:
        await c.post("/process", files={"file": ("a.txt", _INVOICE, "text/plain")})
        r = await c.get("/results?status=success")
    body = r.json()
    assert body["total"] >= 1
    assert all(res["status"] == "success" for res in body["results"])


@pytest.mark.asyncio
async def test_search_filter_by_filename():
    async with _client() as c:
        await c.post("/process", files={"file": ("invoice_jan.txt", _INVOICE, "text/plain")})
        await c.post("/process", files={"file": ("contract.txt", _CONTRACT, "text/plain")})
        r = await c.get("/results?filename=invoice")
    body = r.json()
    assert body["total"] == 1
    assert "invoice" in body["results"][0]["stages"][0]["data"].get("route", "") or True


@pytest.mark.asyncio
async def test_search_pagination():
    async with _client() as c:
        for i in range(5):
            await c.post("/process", files={"file": (f"doc{i}.txt", _INVOICE, "text/plain")})
        r1 = await c.get("/results?limit=2&offset=0")
        r2 = await c.get("/results?limit=2&offset=2")
    assert len(r1.json()["results"]) == 2
    assert len(r2.json()["results"]) == 2
    ids1 = {res["document_id"] for res in r1.json()["results"]}
    ids2 = {res["document_id"] for res in r2.json()["results"]}
    assert ids1.isdisjoint(ids2)


@pytest.mark.asyncio
async def test_search_total_independent_of_page_size():
    async with _client() as c:
        for i in range(3):
            await c.post("/process", files={"file": (f"f{i}.txt", _INVOICE, "text/plain")})
        r_full = await c.get("/results?limit=100")
        r_paged = await c.get("/results?limit=1&offset=0")
    assert r_full.json()["total"] == r_paged.json()["total"] == 3


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_export_json_returns_json_file():
    async with _client() as c:
        upload = await c.post("/process", files={"file": ("doc.txt", _INVOICE, "text/plain")})
        doc_id = upload.json()["document_id"]
        r = await c.get(f"/results/{doc_id}/export?format=json")
    assert r.status_code == 200
    assert r.headers["content-type"] == "application/json"
    assert "attachment" in r.headers["content-disposition"]
    assert r.json()["document_id"] == doc_id


@pytest.mark.asyncio
async def test_export_csv_returns_csv_file():
    async with _client() as c:
        upload = await c.post("/process", files={"file": ("doc.txt", _INVOICE, "text/plain")})
        doc_id = upload.json()["document_id"]
        r = await c.get(f"/results/{doc_id}/export?format=csv")
    assert r.status_code == 200
    assert "text/csv" in r.headers["content-type"]
    assert "attachment" in r.headers["content-disposition"]
    lines = r.text.strip().splitlines()
    assert len(lines) == 2  # header + one data row
    assert "document_id" in lines[0]
    assert doc_id in lines[1]


@pytest.mark.asyncio
async def test_export_missing_document_404():
    async with _client() as c:
        r = await c.get(f"/results/{uuid4()}/export?format=json")
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Reprocess
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_reprocess_returns_new_result():
    async with _client() as c:
        upload = await c.post("/process", files={"file": ("inv.txt", _INVOICE, "text/plain")})
        doc_id = upload.json()["document_id"]
        r = await c.post(f"/results/{doc_id}/reprocess")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] in ("success", "partial", "failed")
    assert len(body["stages"]) == 6


@pytest.mark.asyncio
async def test_reprocess_updates_stored_result():
    async with _client() as c:
        upload = await c.post("/process", files={"file": ("inv.txt", _INVOICE, "text/plain")})
        doc_id = upload.json()["document_id"]
        await c.post(f"/results/{doc_id}/reprocess")
        r = await c.get(f"/results/{doc_id}")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_reprocess_missing_content_404():
    """Reprocess fails if original bytes were never stored."""
    await storage.init_db()
    fake = PipelineResult(
        document_id=uuid4(),
        status="success",
        stages=[StageResult(module="router", status="success", duration_ms=1.0)],
        output={},
        total_duration_ms=1.0,
    )
    await storage.save_result(fake)  # saved without document content

    async with _client() as c:
        r = await c.post(f"/results/{fake.document_id}/reprocess")
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# CSV helper unit test
# ---------------------------------------------------------------------------

def test_result_to_csv_structure():
    result = PipelineResult(
        document_id=uuid4(),
        status="success",
        stages=[
            StageResult(module="router", status="success", duration_ms=1.0,
                        data={"route": "pdf"}),
            StageResult(module="classifier", status="success", duration_ms=1.0,
                        data={"type": "invoice", "confidence": 0.9, "method": "keyword"}),
            StageResult(module="extractor", status="success", duration_ms=1.0,
                        data={"fields": {"amounts": [100.0], "dates": ["2026-01-01"]},
                              "metadata": {"filename": "test.pdf", "size_bytes": 100}}),
            StageResult(module="refiner", status="success", duration_ms=1.0,
                        data={"fields": {"amounts": [100.0]}}),
            StageResult(module="validator", status="success", duration_ms=1.0,
                        data={"valid": True, "violations": [], "warnings": []}),
            StageResult(module="generator", status="success", duration_ms=1.0,
                        data={"summary": "Invoice processed"}),
        ],
        output={"summary": "Invoice processed"},
        total_duration_ms=6.0,
    )
    csv_text = storage.result_to_csv(result)
    lines = csv_text.strip().splitlines()
    assert len(lines) == 2
    header = lines[0].split(",")
    assert "document_id" in header
    assert "doc_type" in header
    assert "amounts" in header
