from __future__ import annotations

import asyncio
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, Response, StreamingResponse

from monitoring_module.store import store

app = FastAPI(title="Document Processor Monitor", version="0.1.0")

_DASHBOARD = (Path(__file__).parent / "dashboard.html").read_text()


@app.get("/", response_class=HTMLResponse)
async def dashboard() -> HTMLResponse:
    return HTMLResponse(_DASHBOARD)


@app.get("/events")
async def sse() -> StreamingResponse:
    async def stream():
        for event in store.recent(50):
            yield event.to_sse()

        q = store.subscribe()
        try:
            while True:
                try:
                    event = await asyncio.wait_for(q.get(), timeout=25)
                    yield event.to_sse()
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        finally:
            store.unsubscribe(q)

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/stats")
async def stats() -> dict:
    return store.stats


@app.get("/history")
async def history(n: int = 100) -> list[dict]:
    import json
    return [json.loads(e.to_sse().removeprefix("data: ").strip()) for e in store.recent(n)]


@app.get("/metrics", response_class=Response)
async def prometheus_metrics() -> Response:
    s = store.stats
    lines: list[str] = [
        "# HELP doc_processor_documents_total Total documents processed through the pipeline",
        "# TYPE doc_processor_documents_total counter",
        f"doc_processor_documents_total {s['total']}",
        "# HELP doc_processor_success_total Successful pipeline completions",
        "# TYPE doc_processor_success_total counter",
        f"doc_processor_success_total {s['success']}",
        "# HELP doc_processor_errors_total Failed pipeline stage runs",
        "# TYPE doc_processor_errors_total counter",
        f"doc_processor_errors_total {s['error']}",
        "# HELP doc_processor_llm_calls_total LLM API calls made (classify + extract)",
        "# TYPE doc_processor_llm_calls_total counter",
        f"doc_processor_llm_calls_total {s.get('llm_calls', 0)}",
        "# HELP doc_processor_avg_duration_ms Average stage duration across all modules",
        "# TYPE doc_processor_avg_duration_ms gauge",
        f"doc_processor_avg_duration_ms {s.get('avg_duration_ms', 0)}",
        "# HELP doc_processor_stage_duration_ms Average duration per pipeline stage",
        "# TYPE doc_processor_stage_duration_ms gauge",
    ]
    for module, m in s.get("by_module", {}).items():
        lines.append(f'doc_processor_stage_duration_ms{{module="{module}"}} {m["avg_ms"]}')

    lines += [
        "# HELP doc_processor_stage_errors_total Error count per pipeline stage",
        "# TYPE doc_processor_stage_errors_total counter",
    ]
    for module, m in s.get("by_module", {}).items():
        lines.append(f'doc_processor_stage_errors_total{{module="{module}"}} {m["error"]}')

    lines += [
        "# HELP doc_processor_route_total Documents processed by detected route",
        "# TYPE doc_processor_route_total counter",
    ]
    for route, count in s.get("by_route", {}).items():
        lines.append(f'doc_processor_route_total{{route="{route}"}} {count}')

    lines += [
        "# HELP doc_processor_doc_type_total Documents classified by type",
        "# TYPE doc_processor_doc_type_total counter",
    ]
    for doc_type, count in s.get("by_doc_type", {}).items():
        lines.append(f'doc_processor_doc_type_total{{doc_type="{doc_type}"}} {count}')

    return Response(
        content="\n".join(lines) + "\n",
        media_type="text/plain; version=0.0.4; charset=utf-8",
    )
