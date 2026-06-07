import pytest

from document_processor.models import Document
from document_processor.registry import build_pipeline


@pytest.mark.asyncio
async def test_pipeline_runs_successfully():
    pipeline = build_pipeline()
    doc = Document(content=b"%PDF-1.4 stub", mimetype="application/pdf", filename="test.pdf")
    result = await pipeline.run(doc)

    assert result.status == "success"
    assert result.document_id == doc.id
    assert len(result.stages) == 6
    assert all(s.status == "success" for s in result.stages)


@pytest.mark.asyncio
async def test_pipeline_routes_by_mimetype():
    pipeline = build_pipeline()
    doc = Document(content=b"col1,col2\n1,2", mimetype="text/csv", filename="data.csv")
    result = await pipeline.run(doc)

    router_stage = next(s for s in result.stages if s.module == "router")
    assert router_stage.data["route"] == "spreadsheet"


@pytest.mark.asyncio
async def test_pipeline_skips_remaining_on_error():
    from document_processor.models import StageResult
    from document_processor.pipeline import Pipeline

    class BrokenModule:
        name = "broken"

        async def process(self, document, context):
            raise RuntimeError("module failure")

        async def health_check(self):
            return False

    from document_processor.modules.stubs import GeneratorModuleStub

    pipeline = Pipeline([BrokenModule(), GeneratorModuleStub()])
    doc = Document(content=b"hello", mimetype="text/plain")
    result = await pipeline.run(doc)

    assert result.status == "failed"
    assert result.stages[0].status == "error"
    assert result.stages[1].status == "skipped"
