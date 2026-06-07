from uuid import UUID

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware

from document_processor import registry
from document_processor.models import Document, PipelineResult

app = FastAPI(title="Document Processor", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

_results: dict[UUID, PipelineResult] = {}


@app.get("/health")
async def health():
    return {"status": "ok", "version": "0.1.0"}


@app.post("/process", response_model=PipelineResult)
async def process_document(file: UploadFile = File(...)):
    content = await file.read()
    document = Document(
        filename=file.filename,
        mimetype=file.content_type or "application/octet-stream",
        content=content,
    )
    pipeline = registry.build_pipeline()
    result = await pipeline.run(document)
    _results[document.id] = result
    return result


@app.get("/results/{document_id}", response_model=PipelineResult)
async def get_result(document_id: UUID):
    result = _results.get(document_id)
    if not result:
        raise HTTPException(status_code=404, detail="Result not found")
    return result


@app.get("/results", response_model=list[PipelineResult])
async def list_results():
    return list(_results.values())
