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


@app.get("/results/stats/timeline", dependencies=[Depends(require_api_key)])
async def results_stats_timeline(
    days: int = Query(30, ge=1, le=365, description="Number of past days to include"),
):
    """Return daily document counts for the last N days (newest first)."""
    return {"days": days, "timeline": await storage.get_stats_timeline(days=days)}


@app.get("/results/stats/stage-timing", dependencies=[Depends(require_api_key)])
async def results_stats_stage_timing():
    """Return per-stage timing stats (avg, min, max, p50, p95) across all results."""
    return {"stages": await storage.get_stats_stage_timing()}


@app.get("/results/stats/error-rates", dependencies=[Depends(require_api_key)])
async def results_stats_error_rates():
    """Return per-doc_type success/failure counts and error rate."""
    return {"error_rates": await storage.get_stats_error_rates()}


@app.get("/results/stats/tags", dependencies=[Depends(require_api_key)])
async def results_stats_tags():
    """Return all tags with their result-count, sorted by count descending."""
    return {"tags": await storage.get_tag_stats()}


@app.get("/results/stats/summary", dependencies=[Depends(require_api_key)])
async def results_stats_summary():
    """Rich aggregate dashboard: flags, priority/workflow breakdown, top tags/labels, time windows."""
    return await storage.get_processing_summary()


@app.get("/results/tags", dependencies=[Depends(require_api_key)])
async def list_all_tags(
    q: str | None = Query(None, description="Prefix filter for tag autocomplete"),
    limit: int = Query(50, ge=1, le=200, description="Maximum number of tags to return"),
):
    """Return unique tag names, optionally filtered by prefix. Useful for autocomplete."""
    return {"tags": await storage.get_all_tags(prefix=q, limit=limit)}


@app.get("/results", dependencies=[Depends(require_api_key)])
async def list_results(
    doc_type: str | None = Query(None, description="Filter by document type (invoice, contract, …)"),
    status: str | None = Query(None, description="Filter by pipeline status (success, partial, failed)"),
    filename: str | None = Query(None, description="Partial filename match"),
    tag: str | None = Query(None, description="Filter by tag (exact, case-insensitive)"),
    label: str | None = Query(None, description="Filter by label name (exact match)"),
    q: str | None = Query(None, description="Keyword search within extracted content"),
    date_from: str | None = Query(None, description="ISO 8601 lower bound for created_at (inclusive)"),
    date_to: str | None = Query(None, description="ISO 8601 upper bound for created_at (inclusive)"),
    pinned: bool | None = Query(None, description="Filter by pin state (true=pinned only, false=unpinned only)"),
    workflow_status: str | None = Query(None, description="Filter by workflow status (pending_review, approved, rejected, archived)"),
    starred: bool | None = Query(None, description="Filter by star state (true=starred only, false=unstarred only)"),
    priority: str | None = Query(None, description="Filter by priority (low, medium, high, critical)"),
    include_expired: bool = Query(False, description="Include results past their expiry date (default: excluded)"),
    sort_by: str = Query("created_at", description="Sort field: created_at, doc_type, filename, pipeline_status"),
    sort_order: str = Query("desc", description="Sort direction: asc or desc"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    if tag:
        results, total = await storage.search_results_by_tag(tag=tag, limit=limit, offset=offset)
    elif label:
        results, total = await storage.search_results_by_label(label_name=label, limit=limit, offset=offset)
    else:
        results, total = await storage.search_results(
            doc_type=doc_type, status=status, filename=filename,
            q=q, date_from=date_from, date_to=date_to,
            pinned=pinned,
            workflow_status=workflow_status,
            starred=starred,
            priority=priority,
            include_expired=include_expired,
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
    format: str = Query("json", description="Export format: json, csv, markdown, or xml"),
):
    result = await storage.get_result(document_id)
    if not result:
        raise HTTPException(status_code=404, detail="Result not found")

    if format == "csv":
        return Response(
            content=storage.result_to_csv(result),
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{document_id}.csv"'},
        )

    if format == "markdown":
        return Response(
            content=storage.result_to_markdown(result),
            media_type="text/markdown",
            headers={"Content-Disposition": f'attachment; filename="{document_id}.md"'},
        )

    if format == "xml":
        return Response(
            content=storage.result_to_xml(result),
            media_type="application/xml",
            headers={"Content-Disposition": f'attachment; filename="{document_id}.xml"'},
        )

    if format != "json":
        raise HTTPException(status_code=400, detail="format must be json, csv, markdown, or xml")

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
    if await storage.is_result_locked(document_id):
        raise HTTPException(status_code=423, detail="Result is locked")
    pipeline = registry.build_pipeline()
    result = await pipeline.run(document)
    await storage.save_result(result, document=document)
    await storage.append_audit(document_id, "reprocessed", f"status={result.status}")
    background_tasks.add_task(_notify_webhooks, "document.reprocessed", result)
    return result


class SnapshotBody(BaseModel):
    label: str | None = None


@app.post("/results/{document_id}/snapshots", status_code=201,
          dependencies=[Depends(require_api_key)])
async def create_snapshot(document_id: UUID, body: SnapshotBody = SnapshotBody()):
    """Save a point-in-time snapshot of the current result."""
    snap = await storage.create_snapshot(document_id, label=body.label)
    if snap is None:
        raise HTTPException(status_code=404, detail="Result not found")
    await storage.append_audit(document_id, "snapshot_created", f"snapshot_id={snap['id']}")
    return snap


@app.get("/results/{document_id}/snapshots", dependencies=[Depends(require_api_key)])
async def list_snapshots(document_id: UUID):
    """List all snapshots for a result (without inline data, newest first)."""
    result = await storage.get_result(document_id)
    if not result:
        raise HTTPException(status_code=404, detail="Result not found")
    snaps = await storage.list_snapshots(document_id)
    return {"document_id": str(document_id), "snapshots": snaps}


@app.get("/results/{document_id}/snapshots/{snapshot_id}",
         dependencies=[Depends(require_api_key)])
async def get_snapshot(document_id: UUID, snapshot_id: int):
    """Fetch a single snapshot (includes full result data at the time of snapshot)."""
    snap = await storage.get_snapshot(document_id, snapshot_id)
    if snap is None:
        raise HTTPException(status_code=404, detail="Snapshot not found")
    return snap


@app.get("/results/{document_id}", response_model=PipelineResult,
         dependencies=[Depends(require_api_key)])
async def get_result(document_id: UUID):
    result = await storage.get_result(document_id)
    if not result:
        raise HTTPException(status_code=404, detail="Result not found")
    await storage.record_result_access(document_id)
    return result


# ---------------------------------------------------------------------------
# Workflow status endpoints
# ---------------------------------------------------------------------------

class WorkflowStatusBody(BaseModel):
    status: str


@app.get("/results/{document_id}/workflow", dependencies=[Depends(require_api_key)])
async def get_workflow_status(document_id: UUID):
    """Return the current workflow status for a result."""
    result = await storage.get_result(document_id)
    if not result:
        raise HTTPException(status_code=404, detail="Result not found")
    ws = await storage.get_workflow_status(document_id)
    return {"document_id": str(document_id), "workflow_status": ws}


@app.put("/results/{document_id}/workflow", status_code=204,
         dependencies=[Depends(require_api_key)])
async def set_workflow_status(document_id: UUID, body: WorkflowStatusBody):
    """Set the workflow status. Allowed values: pending_review, approved, rejected, archived."""
    if body.status not in storage.WORKFLOW_STATUSES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid status. Allowed: {sorted(storage.WORKFLOW_STATUSES)}",
        )
    result = await storage.get_result(document_id)
    if not result:
        raise HTTPException(status_code=404, detail="Result not found")
    await storage.set_workflow_status(document_id, body.status)
    await storage.append_audit(document_id, "workflow_status_set", f"status={body.status}")


@app.delete("/results/{document_id}/workflow", status_code=204,
            dependencies=[Depends(require_api_key)])
async def clear_workflow_status(document_id: UUID):
    """Clear the workflow status (set to null)."""
    result = await storage.get_result(document_id)
    if not result:
        raise HTTPException(status_code=404, detail="Result not found")
    await storage.set_workflow_status(document_id, None)  # type: ignore[arg-type]
    await storage.append_audit(document_id, "workflow_status_cleared")


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
    if await storage.is_result_locked(document_id):
        raise HTTPException(status_code=423, detail="Result is locked")
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


class BulkReprocessBody(BaseModel):
    ids: list[UUID]


@app.post("/results/bulk-reprocess", dependencies=[Depends(require_api_key)])
async def bulk_reprocess_results(body: BulkReprocessBody, background_tasks: BackgroundTasks = BackgroundTasks()):
    """Re-run the pipeline on up to 20 documents concurrently.

    Returns a per-document summary with status (success/skipped/error).
    Documents without stored bytes or that are locked are skipped.
    """
    if not body.ids:
        raise HTTPException(status_code=400, detail="ids must not be empty")
    if len(body.ids) > 20:
        raise HTTPException(status_code=400, detail="Maximum 20 ids per request")

    async def _reprocess_one(doc_id: UUID) -> dict:
        try:
            document = await storage.get_document(doc_id)
            if not document:
                return {"id": str(doc_id), "status": "skipped", "reason": "no_document"}
            if await storage.is_result_locked(doc_id):
                return {"id": str(doc_id), "status": "skipped", "reason": "locked"}
            pipeline = registry.build_pipeline()
            result = await pipeline.run(document)
            await storage.save_result(result, document=document)
            await storage.append_audit(doc_id, "reprocessed", f"status={result.status}")
            background_tasks.add_task(_notify_webhooks, "document.reprocessed", result)
            return {"id": str(doc_id), "status": "success", "pipeline_status": result.status}
        except Exception as exc:
            return {"id": str(doc_id), "status": "error", "reason": str(exc)}

    results = await asyncio.gather(*[_reprocess_one(doc_id) for doc_id in body.ids])
    summary = [r for r in results]
    succeeded = sum(1 for r in summary if r["status"] == "success")
    skipped = sum(1 for r in summary if r["status"] == "skipped")
    errored = sum(1 for r in summary if r["status"] == "error")
    return {
        "requested": len(body.ids),
        "succeeded": succeeded,
        "skipped": skipped,
        "errored": errored,
        "results": summary,
    }


class BatchExportBody(BaseModel):
    ids: list[UUID]
    format: str = "json"


@app.post("/results/batch-export", dependencies=[Depends(require_api_key)])
async def batch_export_results(body: BatchExportBody):
    """Export up to 100 results as a ZIP archive in the requested format."""
    if not body.ids:
        raise HTTPException(status_code=400, detail="ids must not be empty")
    if len(body.ids) > 100:
        raise HTTPException(status_code=400, detail="Maximum 100 ids per request")
    if body.format not in ("json", "csv", "markdown", "xml"):
        raise HTTPException(status_code=400, detail="format must be json, csv, markdown, or xml")
    zip_bytes = await storage.batch_export_zip(body.ids, format=body.format)
    return Response(
        content=zip_bytes,
        media_type="application/zip",
        headers={"Content-Disposition": 'attachment; filename="export.zip"'},
    )


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
# Label registry + per-result label endpoints
# ---------------------------------------------------------------------------

class LabelCreateBody(BaseModel):
    name: str = Field(..., min_length=1, max_length=50)
    color: str = Field("#888888", description="Hex colour string e.g. #ff0000")
    description: str | None = None


class ApplyLabelBody(BaseModel):
    label: str = Field(..., description="Label name to apply")


@app.post("/labels", status_code=201, dependencies=[Depends(require_api_key)])
async def create_label(body: LabelCreateBody):
    """Create a label in the label registry."""
    existing = await storage.get_label(body.name)
    if existing:
        raise HTTPException(status_code=409, detail="Label already exists")
    return await storage.create_label(body.name, body.color, body.description)


@app.get("/labels", dependencies=[Depends(require_api_key)])
async def list_labels():
    """List all labels in the registry."""
    return {"labels": await storage.list_labels()}


@app.get("/labels/{name}", dependencies=[Depends(require_api_key)])
async def get_label(name: str):
    """Retrieve a single label by name."""
    label = await storage.get_label(name)
    if not label:
        raise HTTPException(status_code=404, detail="Label not found")
    return label


@app.delete("/labels/{name}", status_code=204, dependencies=[Depends(require_api_key)])
async def delete_label(name: str):
    """Delete a label and remove it from all results."""
    deleted = await storage.delete_label(name)
    if not deleted:
        raise HTTPException(status_code=404, detail="Label not found")


@app.post("/results/{document_id}/labels", status_code=204,
          dependencies=[Depends(require_api_key)])
async def apply_label_to_result(document_id: UUID, body: ApplyLabelBody):
    """Apply a label to a result (idempotent). Label must exist in the registry."""
    result = await storage.get_result(document_id)
    if not result:
        raise HTTPException(status_code=404, detail="Result not found")
    label = await storage.get_label(body.label)
    if not label:
        raise HTTPException(status_code=404, detail="Label not found")
    await storage.apply_label(document_id, body.label)
    await storage.append_audit(document_id, "label_applied", f"label={body.label}")


@app.delete("/results/{document_id}/labels/{label_name}", status_code=204,
            dependencies=[Depends(require_api_key)])
async def remove_label_from_result(document_id: UUID, label_name: str):
    """Remove a label from a result."""
    result = await storage.get_result(document_id)
    if not result:
        raise HTTPException(status_code=404, detail="Result not found")
    removed = await storage.remove_label_from_result(document_id, label_name)
    if not removed:
        raise HTTPException(status_code=404, detail="Label not assigned to this result")
    await storage.append_audit(document_id, "label_removed", f"label={label_name}")


@app.get("/results/{document_id}/labels", dependencies=[Depends(require_api_key)])
async def get_result_labels(document_id: UUID):
    """List all labels applied to a result."""
    result = await storage.get_result(document_id)
    if not result:
        raise HTTPException(status_code=404, detail="Result not found")
    labels = await storage.get_result_labels(document_id)
    return {"document_id": str(document_id), "labels": labels}


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


@app.post("/webhooks/{webhook_id}/pause", status_code=204,
          dependencies=[Depends(require_api_key)])
async def pause_webhook(webhook_id: UUID):
    """Pause a webhook — deliveries are suspended until resumed."""
    updated = await storage.set_webhook_active(webhook_id, False)
    if not updated:
        raise HTTPException(status_code=404, detail="Webhook not found")


@app.post("/webhooks/{webhook_id}/resume", status_code=204,
          dependencies=[Depends(require_api_key)])
async def resume_webhook(webhook_id: UUID):
    """Resume a paused webhook."""
    updated = await storage.set_webhook_active(webhook_id, True)
    if not updated:
        raise HTTPException(status_code=404, detail="Webhook not found")


@app.post("/webhooks/{webhook_id}/ping", dependencies=[Depends(require_api_key)])
async def ping_webhook(webhook_id: UUID):
    """Send a test ping to the webhook URL to verify it is reachable."""
    from document_processor.webhook_delivery import _deliver_one
    from datetime import datetime, timezone
    import json as _json

    wh = await storage.get_webhook(webhook_id)
    if not wh:
        raise HTTPException(status_code=404, detail="Webhook not found")

    payload = _json.dumps({
        "event": "ping",
        "webhook_id": str(webhook_id),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "message": "This is a test ping from the Document Processor.",
    }).encode()

    success = await _deliver_one(wh, "ping", payload, document_id=None)
    return {"webhook_id": str(webhook_id), "success": success}


@app.get("/webhooks/{webhook_id}/deliveries", dependencies=[Depends(require_api_key)])
async def list_webhook_deliveries(
    webhook_id: UUID,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    """Return paginated delivery log for a webhook."""
    whs = await storage.list_webhooks()
    if not any(str(wh.id) == str(webhook_id) for wh in whs):
        raise HTTPException(status_code=404, detail="Webhook not found")
    deliveries, total = await storage.get_webhook_deliveries(
        str(webhook_id), limit=limit, offset=offset
    )
    return {"webhook_id": str(webhook_id), "total": total, "offset": offset,
            "limit": limit, "deliveries": deliveries}


@app.get("/webhooks/{webhook_id}/deliveries/stats", dependencies=[Depends(require_api_key)])
async def webhook_delivery_stats(webhook_id: UUID):
    """Return aggregate delivery statistics for a webhook."""
    whs = await storage.list_webhooks()
    if not any(str(wh.id) == str(webhook_id) for wh in whs):
        raise HTTPException(status_code=404, detail="Webhook not found")
    stats = await storage.get_webhook_delivery_stats(str(webhook_id))
    return {"webhook_id": str(webhook_id), **stats}


@app.get("/webhooks/stats", dependencies=[Depends(require_api_key)])
async def global_webhook_stats():
    """Return aggregate delivery statistics across all webhooks, with per-event breakdown."""
    return await storage.get_global_webhook_stats()


@app.post("/webhooks/{webhook_id}/deliveries/{delivery_id}/replay",
          dependencies=[Depends(require_api_key)])
async def replay_webhook_delivery(webhook_id: UUID, delivery_id: int):
    """Re-send a prior webhook delivery using the same event and document payload."""
    from document_processor.webhook_delivery import _deliver_one

    wh = await storage.get_webhook(webhook_id)
    if not wh:
        raise HTTPException(status_code=404, detail="Webhook not found")

    delivery = await storage.get_webhook_delivery(delivery_id)
    if not delivery or delivery["webhook_id"] != str(webhook_id):
        raise HTTPException(status_code=404, detail="Delivery not found")

    doc_id_str: str | None = delivery["document_id"]
    result = None
    if doc_id_str:
        try:
            result = await storage.get_result(__import__("uuid").UUID(doc_id_str))
        except Exception:
            pass

    if result is None:
        raise HTTPException(
            status_code=422,
            detail="Original document no longer exists; cannot reconstruct payload",
        )

    import json as _json
    from datetime import datetime, timezone
    payload = _json.dumps({
        "event": delivery["event"],
        "document_id": doc_id_str,
        "status": result.status,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "result": result.model_dump(mode="json"),
    }).encode()

    success = await _deliver_one(wh, delivery["event"], payload, doc_id_str)
    return {
        "webhook_id": str(webhook_id),
        "original_delivery_id": delivery_id,
        "event": delivery["event"],
        "document_id": doc_id_str,
        "success": success,
    }


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
# Result comments
# ---------------------------------------------------------------------------

class CommentBody(BaseModel):
    text: str


@app.post("/results/{document_id}/comments", status_code=201,
          dependencies=[Depends(require_api_key)])
async def add_comment(document_id: UUID, body: CommentBody):
    """Add a comment to a result. Multiple comments are allowed per result."""
    if not body.text.strip():
        raise HTTPException(status_code=400, detail="text must not be empty")
    result = await storage.get_result(document_id)
    if not result:
        raise HTTPException(status_code=404, detail="Result not found")
    comment = await storage.add_comment(document_id, body.text.strip())
    await storage.append_audit(document_id, "comment_added", f"id={comment['id']}")
    return comment


@app.get("/results/{document_id}/comments", dependencies=[Depends(require_api_key)])
async def list_comments(
    document_id: UUID,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    result = await storage.get_result(document_id)
    if not result:
        raise HTTPException(status_code=404, detail="Result not found")
    comments, total = await storage.get_comments(document_id, limit=limit, offset=offset)
    return {
        "document_id": str(document_id),
        "total": total,
        "offset": offset,
        "limit": limit,
        "comments": comments,
    }


@app.delete("/results/{document_id}/comments/{comment_id}", status_code=204,
            dependencies=[Depends(require_api_key)])
async def delete_comment(document_id: UUID, comment_id: int):
    result = await storage.get_result(document_id)
    if not result:
        raise HTTPException(status_code=404, detail="Result not found")
    removed = await storage.delete_comment(document_id, comment_id)
    if not removed:
        raise HTTPException(status_code=404, detail="Comment not found")
    await storage.append_audit(document_id, "comment_deleted", f"id={comment_id}")


# ---------------------------------------------------------------------------
# Custom key-value metadata
# ---------------------------------------------------------------------------

class MetadataValueBody(BaseModel):
    value: object


@app.put("/results/{document_id}/metadata/{key}", status_code=204,
         dependencies=[Depends(require_api_key)])
async def set_metadata(document_id: UUID, key: str, body: MetadataValueBody):
    """Set a metadata key to any JSON-serialisable value."""
    result = await storage.get_result(document_id)
    if not result:
        raise HTTPException(status_code=404, detail="Result not found")
    await storage.set_metadata(document_id, key, body.value)
    await storage.append_audit(document_id, "metadata_set", f"key={key}")


@app.get("/results/{document_id}/metadata", dependencies=[Depends(require_api_key)])
async def get_metadata(document_id: UUID):
    """Return all custom metadata key-value pairs for a result."""
    result = await storage.get_result(document_id)
    if not result:
        raise HTTPException(status_code=404, detail="Result not found")
    meta = await storage.get_metadata(document_id)
    return {"document_id": str(document_id), "metadata": meta}


@app.get("/results/{document_id}/metadata/{key}", dependencies=[Depends(require_api_key)])
async def get_metadata_key(document_id: UUID, key: str):
    """Return a single metadata entry."""
    result = await storage.get_result(document_id)
    if not result:
        raise HTTPException(status_code=404, detail="Result not found")
    entry = await storage.get_metadata_key(document_id, key)
    if entry is None:
        raise HTTPException(status_code=404, detail="Metadata key not found")
    return entry


@app.delete("/results/{document_id}/metadata/{key}", status_code=204,
            dependencies=[Depends(require_api_key)])
async def delete_metadata_key(document_id: UUID, key: str):
    """Delete a single metadata key."""
    result = await storage.get_result(document_id)
    if not result:
        raise HTTPException(status_code=404, detail="Result not found")
    removed = await storage.delete_metadata_key(document_id, key)
    if not removed:
        raise HTTPException(status_code=404, detail="Metadata key not found")
    await storage.append_audit(document_id, "metadata_deleted", f"key={key}")


# ---------------------------------------------------------------------------
# Result relations
# ---------------------------------------------------------------------------

class RelationBody(BaseModel):
    target_id: UUID
    relation: str


@app.post("/results/{document_id}/relations", status_code=201,
          dependencies=[Depends(require_api_key)])
async def add_relation(document_id: UUID, body: RelationBody):
    """Create a typed link between two results."""
    if body.relation not in storage.RELATION_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid relation. Allowed: {sorted(storage.RELATION_TYPES)}",
        )
    if not await storage.get_result(document_id):
        raise HTTPException(status_code=404, detail="Source result not found")
    if not await storage.get_result(body.target_id):
        raise HTTPException(status_code=404, detail="Target result not found")
    entry = await storage.add_relation(document_id, body.target_id, body.relation)
    if entry is None:
        raise HTTPException(status_code=409, detail="Relation already exists")
    await storage.append_audit(document_id, "relation_added",
                               f"relation={body.relation} target={body.target_id}")
    return entry


@app.get("/results/{document_id}/relations", dependencies=[Depends(require_api_key)])
async def list_relations(document_id: UUID):
    """Return all relations where this result is source or target."""
    if not await storage.get_result(document_id):
        raise HTTPException(status_code=404, detail="Result not found")
    relations = await storage.get_relations(document_id)
    return {"document_id": str(document_id), "relations": relations}


@app.delete("/results/{document_id}/relations/{relation_id}", status_code=204,
            dependencies=[Depends(require_api_key)])
async def delete_relation(document_id: UUID, relation_id: int):
    if not await storage.get_result(document_id):
        raise HTTPException(status_code=404, detail="Result not found")
    removed = await storage.delete_relation(document_id, relation_id)
    if not removed:
        raise HTTPException(status_code=404, detail="Relation not found")
    await storage.append_audit(document_id, "relation_deleted", f"id={relation_id}")


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
# Star / unstar endpoints
# ---------------------------------------------------------------------------

@app.put("/results/{document_id}/star", status_code=204,
         dependencies=[Depends(require_api_key)])
async def star_result(document_id: UUID):
    """Mark a result as starred (favourite)."""
    updated = await storage.set_starred(document_id, True)
    if not updated:
        raise HTTPException(status_code=404, detail="Result not found")
    await storage.append_audit(document_id, "starred")


@app.delete("/results/{document_id}/star", status_code=204,
            dependencies=[Depends(require_api_key)])
async def unstar_result(document_id: UUID):
    """Remove the star from a result."""
    state = await storage.is_starred(document_id)
    if state is None:
        raise HTTPException(status_code=404, detail="Result not found")
    await storage.set_starred(document_id, False)
    await storage.append_audit(document_id, "unstarred")


@app.get("/results/{document_id}/star", dependencies=[Depends(require_api_key)])
async def get_star_state(document_id: UUID):
    """Return the star state of a result."""
    state = await storage.is_starred(document_id)
    if state is None:
        raise HTTPException(status_code=404, detail="Result not found")
    return {"starred": state}


# ---------------------------------------------------------------------------
# Priority endpoints
# ---------------------------------------------------------------------------

class PriorityBody(BaseModel):
    priority: str = Field(..., description="Priority level: low, medium, high, or critical")


@app.put("/results/{document_id}/priority", status_code=204,
         dependencies=[Depends(require_api_key)])
async def set_result_priority(document_id: UUID, body: PriorityBody):
    """Set the priority of a result (low, medium, high, critical)."""
    if body.priority not in storage.PRIORITY_LEVELS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid priority. Must be one of: {', '.join(sorted(storage.PRIORITY_LEVELS))}",
        )
    updated = await storage.set_result_priority(document_id, body.priority)
    if not updated:
        raise HTTPException(status_code=404, detail="Result not found")
    await storage.append_audit(document_id, "priority_set", f"priority={body.priority}")


@app.delete("/results/{document_id}/priority", status_code=204,
            dependencies=[Depends(require_api_key)])
async def clear_result_priority(document_id: UUID):
    """Clear the priority of a result."""
    result = await storage.get_result(document_id)
    if not result:
        raise HTTPException(status_code=404, detail="Result not found")
    await storage.set_result_priority(document_id, None)
    await storage.append_audit(document_id, "priority_cleared")


@app.get("/results/{document_id}/priority", dependencies=[Depends(require_api_key)])
async def get_result_priority(document_id: UUID):
    """Return the priority of a result."""
    result = await storage.get_result(document_id)
    if not result:
        raise HTTPException(status_code=404, detail="Result not found")
    p = await storage.get_result_priority(document_id)
    return {"document_id": str(document_id), "priority": p}


# ---------------------------------------------------------------------------
# Expiry (TTL) endpoints
# ---------------------------------------------------------------------------

class ExpiryBody(BaseModel):
    expires_at: str = Field(..., description="ISO 8601 datetime when this result expires (e.g. 2026-12-31T00:00:00Z)")


@app.put("/results/{document_id}/expiry", status_code=204,
         dependencies=[Depends(require_api_key)])
async def set_result_expiry(document_id: UUID, body: ExpiryBody):
    """Set an expiry timestamp on a result. Expired results are hidden from default listings."""
    updated = await storage.set_result_expiry(document_id, body.expires_at)
    if not updated:
        raise HTTPException(status_code=404, detail="Result not found")
    await storage.append_audit(document_id, "expiry_set", f"expires_at={body.expires_at}")


@app.delete("/results/{document_id}/expiry", status_code=204,
            dependencies=[Depends(require_api_key)])
async def clear_result_expiry(document_id: UUID):
    """Clear the expiry timestamp — result will no longer expire."""
    result = await storage.get_result(document_id)
    if not result:
        raise HTTPException(status_code=404, detail="Result not found")
    await storage.set_result_expiry(document_id, None)
    await storage.append_audit(document_id, "expiry_cleared")


@app.get("/results/{document_id}/expiry", dependencies=[Depends(require_api_key)])
async def get_result_expiry(document_id: UUID):
    """Return the expiry timestamp for a result, or null if none is set."""
    result = await storage.get_result(document_id)
    if not result:
        raise HTTPException(status_code=404, detail="Result not found")
    exp = await storage.get_result_expiry(document_id)
    return {"document_id": str(document_id), "expires_at": exp}


@app.post("/admin/purge-expired", dependencies=[Depends(require_api_key)])
async def purge_expired():
    """Delete all results that have passed their expiry timestamp. Returns count deleted."""
    deleted = await storage.purge_expired_results()
    return {"deleted": deleted}


# ---------------------------------------------------------------------------
# Lock / unlock endpoints
# ---------------------------------------------------------------------------

@app.post("/results/{document_id}/lock", status_code=204,
          dependencies=[Depends(require_api_key)])
async def lock_result(document_id: UUID):
    """Lock a result, preventing delete, reprocess, and write operations."""
    updated = await storage.set_result_locked(document_id, True)
    if not updated:
        raise HTTPException(status_code=404, detail="Result not found")
    await storage.append_audit(document_id, "locked")


@app.post("/results/{document_id}/unlock", status_code=204,
          dependencies=[Depends(require_api_key)])
async def unlock_result(document_id: UUID):
    """Unlock a previously locked result."""
    state = await storage.is_result_locked(document_id)
    if state is None:
        raise HTTPException(status_code=404, detail="Result not found")
    await storage.set_result_locked(document_id, False)
    await storage.append_audit(document_id, "unlocked")


@app.get("/results/{document_id}/lock", dependencies=[Depends(require_api_key)])
async def get_lock_state(document_id: UUID):
    """Return the lock state of a result."""
    state = await storage.is_result_locked(document_id)
    if state is None:
        raise HTTPException(status_code=404, detail="Result not found")
    return {"locked": state}


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
# Bookmark endpoints
# ---------------------------------------------------------------------------

class BookmarkBody(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    reference: str = Field(..., min_length=1, max_length=500, description="Opaque reference string (page, line, section, URL fragment…)")
    note: str | None = Field(None, max_length=500)


@app.put("/results/{document_id}/bookmarks/{name}", status_code=200,
         dependencies=[Depends(require_api_key)])
async def upsert_bookmark(document_id: UUID, name: str, body: BookmarkBody):
    """Create or update a named bookmark on a result (name in path overrides body.name)."""
    result = await storage.get_result(document_id)
    if not result:
        raise HTTPException(status_code=404, detail="Result not found")
    bm = await storage.upsert_bookmark(document_id, name, body.reference, body.note)
    await storage.append_audit(document_id, "bookmark_set", f"name={name}")
    return bm


@app.get("/results/{document_id}/bookmarks", dependencies=[Depends(require_api_key)])
async def list_bookmarks(document_id: UUID):
    """List all named bookmarks for a result."""
    result = await storage.get_result(document_id)
    if not result:
        raise HTTPException(status_code=404, detail="Result not found")
    bookmarks = await storage.get_bookmarks(document_id)
    return {"document_id": str(document_id), "bookmarks": bookmarks}


@app.get("/results/{document_id}/bookmarks/{name}", dependencies=[Depends(require_api_key)])
async def get_bookmark(document_id: UUID, name: str):
    """Get a specific named bookmark."""
    result = await storage.get_result(document_id)
    if not result:
        raise HTTPException(status_code=404, detail="Result not found")
    bm = await storage.get_bookmark(document_id, name)
    if not bm:
        raise HTTPException(status_code=404, detail="Bookmark not found")
    return bm


@app.delete("/results/{document_id}/bookmarks/{name}", status_code=204,
            dependencies=[Depends(require_api_key)])
async def delete_bookmark(document_id: UUID, name: str):
    """Delete a named bookmark."""
    result = await storage.get_result(document_id)
    if not result:
        raise HTTPException(status_code=404, detail="Result not found")
    deleted = await storage.delete_bookmark(document_id, name)
    if not deleted:
        raise HTTPException(status_code=404, detail="Bookmark not found")
    await storage.append_audit(document_id, "bookmark_deleted", f"name={name}")


# ---------------------------------------------------------------------------
# Checklist endpoints
# ---------------------------------------------------------------------------

class ChecklistItemBody(BaseModel):
    text: str = Field(..., min_length=1, max_length=500)


@app.post("/results/{document_id}/checklist", status_code=201,
          dependencies=[Depends(require_api_key)])
async def add_checklist_item(document_id: UUID, body: ChecklistItemBody):
    """Add a checklist item to a result."""
    result = await storage.get_result(document_id)
    if not result:
        raise HTTPException(status_code=404, detail="Result not found")
    item = await storage.add_checklist_item(document_id, body.text)
    await storage.append_audit(document_id, "checklist_item_added", f"id={item['id']}")
    return item


@app.get("/results/{document_id}/checklist", dependencies=[Depends(require_api_key)])
async def get_checklist(document_id: UUID):
    """List all checklist items for a result (ordered by position)."""
    result = await storage.get_result(document_id)
    if not result:
        raise HTTPException(status_code=404, detail="Result not found")
    items = await storage.get_checklist(document_id)
    progress = await storage.get_checklist_progress(document_id)
    return {"document_id": str(document_id), "items": items, "progress": progress}


@app.put("/results/{document_id}/checklist/{item_id}/check", status_code=204,
         dependencies=[Depends(require_api_key)])
async def check_item(document_id: UUID, item_id: int):
    """Mark a checklist item as checked."""
    updated = await storage.set_checklist_item_checked(document_id, item_id, True)
    if not updated:
        raise HTTPException(status_code=404, detail="Checklist item not found")
    await storage.append_audit(document_id, "checklist_item_checked", f"id={item_id}")


@app.put("/results/{document_id}/checklist/{item_id}/uncheck", status_code=204,
         dependencies=[Depends(require_api_key)])
async def uncheck_item(document_id: UUID, item_id: int):
    """Mark a checklist item as unchecked."""
    updated = await storage.set_checklist_item_checked(document_id, item_id, False)
    if not updated:
        raise HTTPException(status_code=404, detail="Checklist item not found")


@app.delete("/results/{document_id}/checklist/{item_id}", status_code=204,
            dependencies=[Depends(require_api_key)])
async def delete_checklist_item(document_id: UUID, item_id: int):
    """Delete a checklist item."""
    result = await storage.get_result(document_id)
    if not result:
        raise HTTPException(status_code=404, detail="Result not found")
    deleted = await storage.delete_checklist_item(document_id, item_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Checklist item not found")


@app.get("/results/{document_id}/checklist/progress", dependencies=[Depends(require_api_key)])
async def get_checklist_progress(document_id: UUID):
    """Return total/checked/unchecked counts for a result's checklist."""
    result = await storage.get_result(document_id)
    if not result:
        raise HTTPException(status_code=404, detail="Result not found")
    progress = await storage.get_checklist_progress(document_id)
    return {"document_id": str(document_id), **progress}


# ---------------------------------------------------------------------------
# Reaction endpoints
# ---------------------------------------------------------------------------

class ReactionBody(BaseModel):
    emoji: str = Field(..., description="Reaction emoji key: +1, -1, eyes, check, red_circle")


@app.post("/results/{document_id}/reactions", status_code=200,
          dependencies=[Depends(require_api_key)])
async def add_reaction(document_id: UUID, body: ReactionBody):
    """Increment a reaction counter on a result. Returns updated counts."""
    if body.emoji not in storage.ALLOWED_REACTIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid emoji. Allowed: {', '.join(sorted(storage.ALLOWED_REACTIONS))}",
        )
    result = await storage.get_result(document_id)
    if not result:
        raise HTTPException(status_code=404, detail="Result not found")
    counts = await storage.add_reaction(document_id, body.emoji)
    await storage.append_audit(document_id, "reaction_added", f"emoji={body.emoji}")
    return {"document_id": str(document_id), "reactions": counts}


@app.delete("/results/{document_id}/reactions/{emoji}", status_code=200,
            dependencies=[Depends(require_api_key)])
async def remove_reaction(document_id: UUID, emoji: str):
    """Decrement a reaction counter (minimum 0). Returns updated counts."""
    if emoji not in storage.ALLOWED_REACTIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid emoji. Allowed: {', '.join(sorted(storage.ALLOWED_REACTIONS))}",
        )
    result = await storage.get_result(document_id)
    if not result:
        raise HTTPException(status_code=404, detail="Result not found")
    counts = await storage.remove_reaction(document_id, emoji)
    return {"document_id": str(document_id), "reactions": counts}


@app.get("/results/{document_id}/reactions", dependencies=[Depends(require_api_key)])
async def get_reactions(document_id: UUID):
    """Return all reaction counts for a result."""
    result = await storage.get_result(document_id)
    if not result:
        raise HTTPException(status_code=404, detail="Result not found")
    counts = await storage.get_reactions(document_id)
    return {"document_id": str(document_id), "reactions": counts}


# ---------------------------------------------------------------------------
# Access log endpoints
# ---------------------------------------------------------------------------

@app.get("/results/{document_id}/access-log", dependencies=[Depends(require_api_key)])
async def get_access_log(
    document_id: UUID,
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """Return the access log (GET fetches) for a result, newest first."""
    result = await storage.get_result(document_id)
    if not result:
        raise HTTPException(status_code=404, detail="Result not found")
    entries, total = await storage.get_result_access_log(document_id, limit=limit, offset=offset)
    return {"document_id": str(document_id), "total": total, "entries": entries}


@app.get("/results/{document_id}/access-count", dependencies=[Depends(require_api_key)])
async def get_access_count(document_id: UUID):
    """Return the total number of times a result has been fetched."""
    result = await storage.get_result(document_id)
    if not result:
        raise HTTPException(status_code=404, detail="Result not found")
    count = await storage.get_result_access_count(document_id)
    return {"document_id": str(document_id), "access_count": count}


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


@app.get("/collections/{collection_id}/stats", dependencies=[Depends(require_api_key)])
async def get_collection_stats(collection_id: str):
    """Return aggregate statistics for all results in a collection."""
    stats = await storage.get_collection_stats(collection_id)
    if stats is None:
        raise HTTPException(status_code=404, detail="Collection not found")
    return stats


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


class QuotaBody(BaseModel):
    daily_limit: int = Field(..., ge=1, description="Maximum documents processed per day")


@app.get("/api-keys/{prefix}/quota")
async def get_api_key_quota(prefix: str):
    """Return quota info (daily limit, used today, remaining) for an API key prefix."""
    quota = await storage.get_api_key_quota(prefix)
    if quota is None:
        raise HTTPException(status_code=404, detail="API key not found")
    return quota


@app.put("/api-keys/{prefix}/quota", status_code=204)
async def set_api_key_quota(prefix: str, body: QuotaBody):
    """Set or update the daily document-processing limit for an API key prefix."""
    existing = await storage.get_api_key_quota(prefix)
    if existing is None:
        raise HTTPException(status_code=404, detail="API key not found")
    await storage.set_api_key_quota(prefix, body.daily_limit)


@app.delete("/api-keys/{prefix}/quota", status_code=204)
async def delete_api_key_quota(prefix: str):
    """Remove the daily limit for an API key prefix."""
    existing = await storage.get_api_key_quota(prefix)
    if existing is None:
        raise HTTPException(status_code=404, detail="API key not found")
    await storage.delete_api_key_quota(prefix)
