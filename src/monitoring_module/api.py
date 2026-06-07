from __future__ import annotations

import asyncio
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, StreamingResponse

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
