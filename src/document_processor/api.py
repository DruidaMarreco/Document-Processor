from __future__ import annotations

import asyncio
import json
import secrets
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import UUID

import json as _json

from fastapi import BackgroundTasks, Depends, FastAPI, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, Response, StreamingResponse
from pydantic import BaseModel, Field
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

from document_processor import registry, storage
from document_processor.auth import require_api_key
from document_processor.config import settings
from document_processor.job_worker import run_worker
from document_processor.scheduler import run_scheduler
from document_processor.models import ApiKey, ApiKeyInfo, Document, JobRecord, PipelineResult, WebhookConfig
from document_processor.retention import run_retention
from document_processor.webhook_delivery import fire_webhooks
from monitoring_module.api import app as _monitoring_app

_UPLOAD_HTML = (Path(__file__).parent / "upload.html").read_text()

limiter = Limiter(key_func=get_remote_address, default_limits=["60/minute"])

_job_queue: asyncio.Queue = asyncio.Queue()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    await storage.init_db()
    tasks = [asyncio.create_task(run_worker(_job_queue)),
             asyncio.create_task(run_scheduler(_job_queue))]
    if settings.retention_days > 0:
        tasks.append(asyncio.create_task(run_retention(settings.retention_days)))
    yield
    for t in tasks:
        t.cancel()
    for t in tasks:
        try:
            await t
        except asyncio.CancelledError:
            pass


app = FastAPI(title="Document Processor", version="0.1.0", lifespan=lifespan)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/monitoring", _monitoring_app)


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def root() -> HTMLResponse:
    return HTMLResponse(_UPLOAD_HTML)


@app.get("/upload", response_class=HTMLResponse, include_in_schema=False)
async def upload_page() -> HTMLResponse:
    return HTMLResponse(_UPLOAD_HTML)


@app.get("/health")
async def health():
    n = await storage.count_results()
    return {"status": "ok", "version": "0.1.0", "stored_results": n}


# ---------------------------------------------------------------------------
# Process endpoints
# ---------------------------------------------------------------------------

async def _notify_webhooks(event: str, result: PipelineResult) -> None:
    webhooks = await storage.list_webhooks(active_only=True)
    await fire_webhooks(webhooks, event, result)


def _parse_config(raw: str | None) -> dict:
    if not raw:
        return {}
    try:
        parsed = _json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


async def _resolve_config(config_str: str | None, template: str | None) -> dict:
    """Merge template config (lower priority) with inline config (higher priority)."""
    base: dict = {}
    if template:
        tmpl = await storage.get_template(template)
        if tmpl:
            base = dict(tmpl["config"])
    inline = _parse_config(config_str)
    base.update(inline)
    return base


@app.post("/process", response_model=PipelineResult, dependencies=[Depends(require_api_key)])
@limiter.limit("30/minute")
async def process_document(
    request: Request,
    file: UploadFile = File(...),
    background_tasks: BackgroundTasks = BackgroundTasks(),
    dedup: bool = Query(False, description="Return cached result for identical content"),
    config: str | None = Form(None, description="Optional JSON pipeline config overrides"),
    template: str | None = Query(None, description="Named processing template to use as base config"),
):
    content = await file.read()
    if dedup:
        cached = await storage.find_duplicate(content)
        if cached:
            return Response(
                content=cached.model_dump_json(),
                media_type="application/json",
                headers={"X-Dedup-Result": "true", "X-Document-Id": str(cached.document_id)},
            )
    document = Document(
        filename=file.filename,
        mimetype=file.content_type or "application/octet-stream",
        content=content,
    )
    pipeline = registry.build_pipeline()
    seed_ctx = {"_config": await _resolve_config(config, template)}
    result = await pipeline.run(document, context=seed_ctx)
    await storage.save_result(result, document=document)
    await storage.append_audit(result.document_id, "processed",
                               f"status={result.status} filename={file.filename}")
    event = "document.processed" if result.status != "failed" else "document.failed"
    background_tasks.add_task(_notify_webhooks, event, result)
    return result


@app.post("/process/stream", dependencies=[Depends(require_api_key)])
@limiter.limit("30/minute")
async def process_stream(
    request: Request,
    file: UploadFile = File(...),
    config: str | None = Form(None, description="Optional JSON pipeline config overrides"),
):
    """Process a single document and stream SSE events for each pipeline stage."""
    content = await file.read()
    document = Document(
        filename=file.filename,
        mimetype=file.content_type or "application/octet-stream",
        content=content,
    )
    pipeline = registry.build_pipeline()
    seed_ctx = {"_config": _parse_config(config)}
    stage_queue: asyncio.Queue = asyncio.Queue()

    async def _capture(_doc: Document, result) -> None:
        await stage_queue.put(result)

    pipeline.add_listener(_capture)

    async def generate():
        task = asyncio.create_task(pipeline.run(document, context=seed_ctx))
        n_stages = len(pipeline.stages)

        for _ in range(n_stages):
            try:
                result = await asyncio.wait_for(stage_queue.get(), timeout=120.0)
                payload = json.dumps({"type": "stage", **result.model_dump(mode="json")})
                yield f"data: {payload}\n\n"
            except asyncio.TimeoutError:
                yield ": keepalive\n\n"

        final = await task
        await storage.save_result(final, document=document)
        event = "document.processed" if final.status != "failed" else "document.failed"
        asyncio.create_task(_notify_webhooks(event, final))
        payload = json.dumps({"type": "done", **final.model_dump(mode="json")})
        yield f"data: {payload}\n\n"

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.post("/batch", dependencies=[Depends(require_api_key)])
@limiter.limit("10/minute")
async def batch_process(
    request: Request,
    files: list[UploadFile] = File(...),
    background_tasks: BackgroundTasks = BackgroundTasks(),
):
    """Process multiple documents concurrently."""
    if not files:
        raise HTTPException(status_code=400, detail="No files provided")
    if len(files) > 20:
        raise HTTPException(status_code=400, detail="Maximum 20 files per batch")

    async def _process_one(file: UploadFile) -> dict:
        content = await file.read()
        document = Document(
            filename=file.filename,
            mimetype=file.content_type or "application/octet-stream",
            content=content,
        )
        pipeline = registry.build_pipeline()
        try:
            result = await pipeline.run(document)
            await storage.save_result(result, document=document)
            event = "document.processed" if result.status != "failed" else "document.failed"
            background_tasks.add_task(_notify_webhooks, event, result)
            return result.model_dump(mode="json")
        except Exception as exc:
            return {"filename": file.filename, "status": "failed", "error": str(exc)}

    raw = await asyncio.gather(*[_process_one(f) for f in files], return_exceptions=True)
    return [
        {"status": "failed", "error": str(r)} if isinstance(r, Exception) else r
        for r in raw
    ]


# ---------------------------------------------------------------------------
# Results endpoints
# ---------------------------------------------------------------------------

@app.get("/results/stats", dependencies=[Depends(require_api_key)])
async def results_stats():
    """Aggregate statistics: counts by status/doc_type, avg duration, last 24 h."""
    return await storage.get_stats()


@app.get("/results", dependencies=[Depends(require_api_key)])
async def list_results(
    doc_type: str | None = Query(None, description="Filter by document type (invoice, contract, …)"),
    status: str | None = Query(None, description="Filter by pipeline status (success, partial, failed)"),
    filename: str | None = Query(None, description="Partial filename match"),
    tag: str | None = Query(None, description="Filter by tag (exact, case-insensitive)"),
    q: str | None = Query(None, description="Keyword search within extracted content"),
    date_from: str | None = Query(None, description="ISO 8601 lower bound for created_at (inclusive)"),
    date_to: str | None = Query(None, description="ISO 8601 upper bound for created_at (inclusive)"),
    pinned: bool | None = Query(None, description="Filter by pin state (true=pinned only, false=unpinned only)"),
    sort_by: str = Query("created_at", description="Sort field: created_at, doc_type, filename, pipeline_status"),
    sort_order: str = Query("desc", description="Sort direction: asc or desc"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    if tag:
        results, total = await storage.search_results_by_tag(tag=tag, limit=limit, offset=offset)
    else:
        results, total = await storage.search_results(
            doc_type=doc_type, status=status, filename=filename,
            q=q, date_from=date_from, date_to=date_to,
            pinned=pinned,
            sort_by=sort_by, sort_order=sort_order,
            limit=limit, offset=offset,
        )
    return {
        "total": total,
        "offset": offset,
        "limit": limit,
        "results": [r.model_dump(mode="json") for r in results],
    }


@app.get("/results/{document_id}/export", dependencies=[Depends(require_api_key)])
async def export_result(
    document_id: UUID,
    format: str = Query("json", description="Export format: json or csv"),
):
    result = await storage.get_result(document_id)
    if not result:
        raise HTTPException(status_code=404, detail="Result not found")

    if format == "csv":
        csv_content = storage.result_to_csv(result)
        return Response(
            content=csv_content,
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{document_id}.csv"'},
        )

    return Response(
        content=result.model_dump_json(indent=2),
        media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="{document_id}.json"'},
    )


@app.post("/results/{document_id}/reprocess", response_model=PipelineResult,
          dependencies=[Depends(require_api_key)])
async def reprocess_document(document_id: UUID, background_tasks: BackgroundTasks = BackgroundTasks()):
    """Re-run the full pipeline on the original stored document bytes."""
    document = await storage.get_document(document_id)
    if not document:
        raise HTTPException(
            status_code=404,
            detail="Original document content not found. "
                   "Only documents uploaded via /process, /process/stream, or /batch can be reprocessed.",
        )
    pipeline = registry.build_pipeline()
    result = await pipeline.run(document)
    await storage.save_result(result, document=document)
    await storage.append_audit(document_id, "reprocessed", f"status={result.status}")
    background_tasks.add_task(_notify_webhooks, "document.reprocessed", result)
    return result


@app.get("/results/{document_id}", response_model=PipelineResult,
         dependencies=[Depends(require_api_key)])
async def get_result(document_id: UUID):
    result = await storage.get_result(document_id)
    if not result:
        raise HTTPException(status_code=404, detail="Result not found")
    return result


def _stage_summary(result: PipelineResult) -> dict:
    """Flatten stage data into a summary dict for diffing."""
    summary: dict = {
        "document_id": str(result.document_id),
        "status": result.status,
        "total_duration_ms": result.total_duration_ms,
    }
    for stage in result.stages:
        summary[stage.module] = {
            "status": stage.status,
            "data": stage.data,
            "errors": stage.errors,
        }
    return summary


def _diff_values(a, b) -> dict:
    if a == b:
        return {"changed": False, "left": a, "right": b}
    return {"changed": True, "left": a, "right": b}


def _diff_dicts(left: dict, right: dict) -> dict:
    all_keys = set(left) | set(right)
    return {
        k: _diff_values(left.get(k), right.get(k))
        for k in sorted(all_keys)
    }


@app.get("/results/{document_id}/similar", dependencies=[Depends(require_api_key)])
async def find_similar_results(
    document_id: UUID,
    top_n: int = Query(5, ge=1, le=50, description="Number of similar results to return"),
    min_score: float = Query(0.0, ge=0.0, le=1.0, description="Minimum Jaccard similarity score"),
):
    """Find the most similar stored results based on extracted field-token overlap."""
    result = await storage.get_result(document_id)
    if not result:
        raise HTTPException(status_code=404, detail="Result not found")
    similar = await storage.find_similar(document_id, top_n=top_n, min_score=min_score)
    return {"document_id": str(document_id), "similar": similar}


@app.get("/results/{document_id}/confidence", dependencies=[Depends(require_api_key)])
async def get_field_confidence(document_id: UUID):
    """Return per-field confidence scores for a result's extracted fields."""
    result = await storage.get_result(document_id)
    if not result:
        raise HTTPException(status_code=404, detail="Result not found")
    field_confidence: dict = {}
    overall_confidence: float | None = None
    for stage in result.stages:
        if stage.module == "classifier":
            overall_confidence = stage.data.get("confidence")
        if stage.module == "generator":
            field_confidence = (stage.data.get("output") or {}).get("field_confidence", {})
    return {
        "document_id": str(document_id),
        "overall_confidence": overall_confidence,
        "field_confidence": field_confidence,
        "field_count": len(field_confidence),
    }


@app.get("/results/{document_id}/diff/{other_id}", dependencies=[Depends(require_api_key)])
async def diff_results(document_id: UUID, other_id: UUID):
    """Compare two pipeline results. Returns per-field diff (changed, left, right)."""
    left, right = await asyncio.gather(
        storage.get_result(document_id),
        storage.get_result(other_id),
    )
    if not left:
        raise HTTPException(status_code=404, detail=f"Result {document_id} not found")
    if not right:
        raise HTTPException(status_code=404, detail=f"Result {other_id} not found")

    left_summary = _stage_summary(left)
    right_summary = _stage_summary(right)

    top_keys = {"document_id", "status", "total_duration_ms"}
    top_diff = {k: _diff_values(left_summary.get(k), right_summary.get(k)) for k in top_keys}

    stage_names = {s.module for s in left.stages} | {s.module for s in right.stages}
    stage_diff: dict = {}
    for name in sorted(stage_names):
        ls = left_summary.get(name, {})
        rs = right_summary.get(name, {})
        stage_diff[name] = {
            "status": _diff_values(ls.get("status"), rs.get("status")),
            "data": _diff_dicts(ls.get("data") or {}, rs.get("data") or {}),
        }

    changed_count = sum(
        1 for v in {**top_diff, **{k: v for s in stage_diff.values()
                                    for k, v in s.get("data", {}).items()}}.values()
        if isinstance(v, dict) and v.get("changed")
    )

    return {
        "left_id": str(document_id),
        "right_id": str(other_id),
        "changed_fields": changed_count,
        "top": top_diff,
        "stages": stage_diff,
    }


@app.delete("/results/{document_id}", status_code=204, dependencies=[Depends(require_api_key)])
async def delete_result(document_id: UUID):
    """Delete a stored result and its original document bytes."""
    result = await storage.get_result(document_id)
    if not result:
        raise HTTPException(status_code=404, detail="Result not found")
    await storage.append_audit(document_id, "deleted")
    await storage.delete_result(document_id)


# ---------------------------------------------------------------------------
# Bulk operations
# ---------------------------------------------------------------------------

class BulkDeleteBody(BaseModel):
    ids: list[UUID]


class BulkTagBody(BaseModel):
    ids: list[UUID]
    tags: list[str]


@app.post("/results/bulk-delete", dependencies=[Depends(require_api_key)])
async def bulk_delete_results(body: BulkDeleteBody):
    """Delete up to 100 results in one request. Returns count of results actually deleted."""
    if not body.ids:
        raise HTTPException(status_code=400, detail="ids must not be empty")
    if len(body.ids) > 100:
        raise HTTPException(status_code=400, detail="Maximum 100 ids per request")
    deleted = await storage.bulk_delete_results(body.ids)
    return {"deleted": deleted, "requested": len(body.ids)}


@app.post("/results/bulk-tag", dependencies=[Depends(require_api_key)])
async def bulk_tag_results(body: BulkTagBody):
    """Add tags to up to 100 results in one request. Non-existent IDs are silently skipped."""
    if not body.ids:
        raise HTTPException(status_code=400, detail="ids must not be empty")
    if len(body.ids) > 100:
        raise HTTPException(status_code=400, detail="Maximum 100 ids per request")
    if not body.tags:
        raise HTTPException(status_code=400, detail="tags must not be empty")
    updated = await storage.bulk_add_tags(body.ids, body.tags)
    return {"updated": updated, "requested": len(body.ids)}


# ---------------------------------------------------------------------------
# Tagging endpoints
# ---------------------------------------------------------------------------

class TagsBody(BaseModel):
    tags: list[str]


@app.post("/results/{document_id}/tags", status_code=204,
          dependencies=[Depends(require_api_key)])
async def add_tags(document_id: UUID, body: TagsBody):
    result = await storage.get_result(document_id)
    if not result:
        raise HTTPException(status_code=404, detail="Result not found")
    await storage.add_tags(document_id, body.tags)
    await storage.append_audit(document_id, "tags_added", f"tags={body.tags}")


@app.get("/results/{document_id}/tags", dependencies=[Depends(require_api_key)])
async def get_tags(document_id: UUID):
    result = await storage.get_result(document_id)
    if not result:
        raise HTTPException(status_code=404, detail="Result not found")
    return {"tags": await storage.get_tags(document_id)}


@app.delete("/results/{document_id}/tags/{tag}", status_code=204,
            dependencies=[Depends(require_api_key)])
async def remove_tag(document_id: UUID, tag: str):
    result = await storage.get_result(document_id)
    if not result:
        raise HTTPException(status_code=404, detail="Result not found")
    removed = await storage.remove_tag(document_id, tag)
    if not removed:
        raise HTTPException(status_code=404, detail="Tag not found")


# ---------------------------------------------------------------------------
# Async job queue endpoints
# ---------------------------------------------------------------------------

@app.post("/jobs", response_model=JobRecord, status_code=202,
          dependencies=[Depends(require_api_key)])
@limiter.limit("30/minute")
async def submit_job(request: Request, file: UploadFile = File(...)):
    """Submit a document for background processing. Poll GET /jobs/{job_id} for status."""
    content = await file.read()
    document = Document(
        filename=file.filename,
        mimetype=file.content_type or "application/octet-stream",
        content=content,
    )
    job = JobRecord(
        status="queued",
        filename=file.filename,
        mimetype=file.content_type or "application/octet-stream",
    )
    # Use job_id as document id so get_job_document can reuse get_document
    document = Document(
        id=job.job_id,
        filename=file.filename,
        mimetype=file.content_type or "application/octet-stream",
        content=content,
    )
    await storage.create_job(job, document)
    await _job_queue.put(job.job_id)
    return job


@app.get("/jobs", response_model=list[JobRecord], dependencies=[Depends(require_api_key)])
async def list_jobs(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    return await storage.list_jobs(limit=limit, offset=offset)


@app.get("/jobs/{job_id}", response_model=JobRecord, dependencies=[Depends(require_api_key)])
async def get_job(job_id: UUID):
    job = await storage.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


# ---------------------------------------------------------------------------
# Webhook endpoints
# ---------------------------------------------------------------------------

class WebhookCreate(BaseModel):
    url: str
    events: list[str] = ["document.processed", "document.failed", "document.reprocessed"]
    doc_types: list[str] = Field(default_factory=list,
                                  description="If non-empty, only fire for these doc_types")
    secret: str | None = None


@app.post("/webhooks", response_model=WebhookConfig, status_code=201,
          dependencies=[Depends(require_api_key)])
async def create_webhook(body: WebhookCreate):
    wh = WebhookConfig(url=body.url, events=body.events, doc_types=body.doc_types, secret=body.secret)
    await storage.save_webhook(wh)
    return wh


@app.get("/webhooks", response_model=list[WebhookConfig],
         dependencies=[Depends(require_api_key)])
async def list_webhooks():
    return await storage.list_webhooks()


@app.delete("/webhooks/{webhook_id}", status_code=204,
            dependencies=[Depends(require_api_key)])
async def delete_webhook(webhook_id: UUID):
    deleted = await storage.delete_webhook(webhook_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Webhook not found")


# ---------------------------------------------------------------------------
# Document notes
# ---------------------------------------------------------------------------

class NoteBody(BaseModel):
    note: str


@app.put("/results/{document_id}/note", status_code=204,
         dependencies=[Depends(require_api_key)])
async def set_note(document_id: UUID, body: NoteBody):
    result = await storage.get_result(document_id)
    if not result:
        raise HTTPException(status_code=404, detail="Result not found")
    await storage.set_note(document_id, body.note)
    await storage.append_audit(document_id, "note_set")


@app.get("/results/{document_id}/note", dependencies=[Depends(require_api_key)])
async def get_note(document_id: UUID):
    result = await storage.get_result(document_id)
    if not result:
        raise HTTPException(status_code=404, detail="Result not found")
    note = await storage.get_note(document_id)
    if note is None:
        raise HTTPException(status_code=404, detail="No note set for this result")
    return note


@app.delete("/results/{document_id}/note", status_code=204,
            dependencies=[Depends(require_api_key)])
async def delete_note(document_id: UUID):
    result = await storage.get_result(document_id)
    if not result:
        raise HTTPException(status_code=404, detail="Result not found")
    removed = await storage.delete_note(document_id)
    if not removed:
        raise HTTPException(status_code=404, detail="No note set for this result")


# ---------------------------------------------------------------------------
# Pin / unpin endpoints
# ---------------------------------------------------------------------------

@app.put("/results/{document_id}/pin", status_code=204,
         dependencies=[Depends(require_api_key)])
async def pin_result(document_id: UUID):
    """Pin a result so it is excluded from retention cleanup."""
    updated = await storage.set_pinned(document_id, True)
    if not updated:
        raise HTTPException(status_code=404, detail="Result not found")
    await storage.append_audit(document_id, "pinned")


@app.delete("/results/{document_id}/pin", status_code=204,
            dependencies=[Depends(require_api_key)])
async def unpin_result(document_id: UUID):
    """Unpin a previously pinned result."""
    state = await storage.is_pinned(document_id)
    if state is None:
        raise HTTPException(status_code=404, detail="Result not found")
    if not state:
        raise HTTPException(status_code=409, detail="Result is not pinned")
    await storage.set_pinned(document_id, False)
    await storage.append_audit(document_id, "unpinned")


@app.get("/results/{document_id}/pin", dependencies=[Depends(require_api_key)])
async def get_pin_state(document_id: UUID):
    """Return the pin state of a result."""
    state = await storage.is_pinned(document_id)
    if state is None:
        raise HTTPException(status_code=404, detail="Result not found")
    return {"pinned": state}


# ---------------------------------------------------------------------------
# Audit log endpoint
# ---------------------------------------------------------------------------

@app.get("/results/{document_id}/audit", dependencies=[Depends(require_api_key)])
async def get_audit_log(
    document_id: UUID,
    limit: int = Query(100, ge=1, le=500),
):
    """Return the immutable audit log for a result (oldest first)."""
    result = await storage.get_result(document_id)
    if not result:
        raise HTTPException(status_code=404, detail="Result not found")
    entries = await storage.get_audit_log(document_id, limit=limit)
    return {"document_id": str(document_id), "entries": entries}


# ---------------------------------------------------------------------------
# Scheduled processing
# ---------------------------------------------------------------------------

@app.post("/schedule", status_code=201, dependencies=[Depends(require_api_key)])
@limiter.limit("30/minute")
async def schedule_document(
    request: Request,
    file: UploadFile = File(...),
    run_at: str = Query(..., description="ISO 8601 datetime when to process (e.g. 2026-06-10T09:00:00Z)"),
    config: str | None = Form(None, description="Optional JSON pipeline config overrides"),
):
    """Schedule a document for processing at a future time."""
    from uuid import uuid4 as _uuid4
    content = await file.read()
    document = Document(
        filename=file.filename,
        mimetype=file.content_type or "application/octet-stream",
        content=content,
    )
    sched_id = str(_uuid4())
    cfg = _parse_config(config) or None
    result = await storage.create_scheduled_job(sched_id, run_at, document, config=cfg)
    return result


@app.get("/schedule", dependencies=[Depends(require_api_key)])
async def list_scheduled(
    status: str | None = Query(None, description="Filter by status: pending, dispatched, failed"),
):
    return await storage.list_scheduled_jobs(status=status)


@app.get("/schedule/{scheduled_id}", dependencies=[Depends(require_api_key)])
async def get_scheduled(scheduled_id: str):
    job = await storage.get_scheduled_job(scheduled_id)
    if not job:
        raise HTTPException(status_code=404, detail="Scheduled job not found")
    return job


@app.delete("/schedule/{scheduled_id}", status_code=204, dependencies=[Depends(require_api_key)])
async def cancel_scheduled(scheduled_id: str):
    """Cancel a pending scheduled job. Returns 409 if already dispatched."""
    sched = await storage.get_scheduled_job(scheduled_id)
    if not sched:
        raise HTTPException(status_code=404, detail="Scheduled job not found")
    if sched["status"] != "pending":
        raise HTTPException(status_code=409, detail=f"Cannot cancel: status is '{sched['status']}'")
    await storage.delete_scheduled_job(scheduled_id)


# ---------------------------------------------------------------------------
# Processing templates
# ---------------------------------------------------------------------------

class TemplateCreate(BaseModel):
    name: str
    config: dict
    description: str | None = None


@app.post("/templates", status_code=201, dependencies=[Depends(require_api_key)])
async def create_or_update_template(body: TemplateCreate):
    """Create or update a named processing template (upsert by name)."""
    return await storage.save_template(body.name, body.config, body.description)


@app.get("/templates", dependencies=[Depends(require_api_key)])
async def list_templates():
    return await storage.list_templates()


@app.get("/templates/{name}", dependencies=[Depends(require_api_key)])
async def get_template(name: str):
    tmpl = await storage.get_template(name)
    if not tmpl:
        raise HTTPException(status_code=404, detail="Template not found")
    return tmpl


@app.delete("/templates/{name}", status_code=204, dependencies=[Depends(require_api_key)])
async def delete_template(name: str):
    deleted = await storage.delete_template(name)
    if not deleted:
        raise HTTPException(status_code=404, detail="Template not found")


# ---------------------------------------------------------------------------
# Collections
# ---------------------------------------------------------------------------

class CollectionCreate(BaseModel):
    name: str
    description: str | None = None


class CollectionAddBody(BaseModel):
    ids: list[UUID]


@app.post("/collections", status_code=201, dependencies=[Depends(require_api_key)])
async def create_collection(body: CollectionCreate):
    """Create a named collection for grouping related results."""
    return await storage.create_collection(body.name, body.description)


@app.get("/collections", dependencies=[Depends(require_api_key)])
async def list_collections():
    return await storage.list_collections()


@app.get("/collections/{collection_id}", dependencies=[Depends(require_api_key)])
async def get_collection(collection_id: str):
    col = await storage.get_collection(collection_id)
    if not col:
        raise HTTPException(status_code=404, detail="Collection not found")
    return col


@app.delete("/collections/{collection_id}", status_code=204,
            dependencies=[Depends(require_api_key)])
async def delete_collection(collection_id: str):
    deleted = await storage.delete_collection(collection_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Collection not found")


@app.post("/collections/{collection_id}/members", status_code=204,
          dependencies=[Depends(require_api_key)])
async def add_to_collection(collection_id: str, body: CollectionAddBody):
    col = await storage.get_collection(collection_id)
    if not col:
        raise HTTPException(status_code=404, detail="Collection not found")
    if not body.ids:
        raise HTTPException(status_code=400, detail="ids must not be empty")
    if len(body.ids) > 100:
        raise HTTPException(status_code=400, detail="Maximum 100 ids per request")
    await storage.add_to_collection(collection_id, body.ids)


@app.delete("/collections/{collection_id}/members/{result_id}", status_code=204,
            dependencies=[Depends(require_api_key)])
async def remove_from_collection(collection_id: str, result_id: UUID):
    col = await storage.get_collection(collection_id)
    if not col:
        raise HTTPException(status_code=404, detail="Collection not found")
    removed = await storage.remove_from_collection(collection_id, result_id)
    if not removed:
        raise HTTPException(status_code=404, detail="Result not in collection")


@app.get("/collections/{collection_id}/members", dependencies=[Depends(require_api_key)])
async def list_collection_members(
    collection_id: str,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    col = await storage.get_collection(collection_id)
    if not col:
        raise HTTPException(status_code=404, detail="Collection not found")
    results, total = await storage.list_collection_results(collection_id, limit=limit, offset=offset)
    return {
        "collection_id": collection_id,
        "total": total,
        "offset": offset,
        "limit": limit,
        "results": [r.model_dump(mode="json") for r in results],
    }


# ---------------------------------------------------------------------------
# Admin endpoints
# ---------------------------------------------------------------------------

@app.post("/admin/cleanup", dependencies=[Depends(require_api_key)])
async def manual_cleanup(
    older_than_days: int = Query(..., ge=1, description="Delete results older than N days"),
):
    """Manually trigger retention cleanup. Returns number of results deleted."""
    deleted = await storage.cleanup_old_results(older_than_days)
    return {"deleted": deleted, "older_than_days": older_than_days}


@app.get("/admin/retention", dependencies=[Depends(require_api_key)])
async def retention_config():
    """Return current retention policy configuration."""
    return {
        "retention_days": settings.retention_days,
        "active": settings.retention_days > 0,
    }


# ---------------------------------------------------------------------------
# API key management endpoints  (no auth required — bootstrap path)
# ---------------------------------------------------------------------------

class ApiKeyCreate(BaseModel):
    name: str


@app.post("/api-keys", response_model=ApiKey, status_code=201)
async def create_api_key(body: ApiKeyCreate):
    """Create a new API key. The full key is returned only once."""
    key = secrets.token_urlsafe(32)
    api_key = ApiKey(key=key, name=body.name)
    await storage.save_api_key(api_key)
    return api_key


@app.get("/api-keys", response_model=list[ApiKeyInfo])
async def list_api_keys():
    """List all registered API keys (prefixes only — full keys are never re-exposed)."""
    return await storage.list_api_keys()


@app.delete("/api-keys/{prefix}", status_code=204)
async def delete_api_key(prefix: str):
    deleted = await storage.delete_api_key_by_prefix(prefix)
    if not deleted:
        raise HTTPException(status_code=404, detail="API key not found")
