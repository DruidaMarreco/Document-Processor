import asyncio

import pytest

from document_processor.models import Document, StageResult
from monitoring_module.store import EventStore, PipelineEvent
from monitoring_module.monitor import MonitoringModule


@pytest.mark.asyncio
async def test_store_records_event():
    store = EventStore()
    event = PipelineEvent(
        document_id="abc", module="router", status="success",
        data={"route": "pdf"}, errors=[], duration_ms=1.5,
    )
    await store.record(event)
    assert store.stats["total"] == 1
    assert store.stats["success"] == 1
    assert store.stats["by_route"]["pdf"] == 1


@pytest.mark.asyncio
async def test_store_tracks_errors():
    store = EventStore()
    for status in ("success", "success", "error"):
        await store.record(PipelineEvent(
            document_id="x", module="extractor", status=status,
            data={}, errors=[], duration_ms=10.0,
        ))
    assert store.stats["error"] == 1
    assert round(store.stats["success_rate"], 1) == 66.7


@pytest.mark.asyncio
async def test_store_sse_broadcast():
    store = EventStore()
    q = store.subscribe()
    event = PipelineEvent(
        document_id="doc1", module="classifier", status="success",
        data={}, errors=[], duration_ms=5.0,
    )
    await store.record(event)
    received = await asyncio.wait_for(q.get(), timeout=1.0)
    assert received.module == "classifier"
    store.unsubscribe(q)


@pytest.mark.asyncio
async def test_store_recent_bounded():
    store = EventStore(maxlen=5)
    for i in range(10):
        await store.record(PipelineEvent(
            document_id=str(i), module="router", status="success",
            data={}, errors=[], duration_ms=1.0,
        ))
    assert len(store.recent(100)) == 5


@pytest.mark.asyncio
async def test_monitoring_module_records_stage_result():
    from monitoring_module.store import EventStore
    import monitoring_module.store as store_module
    original = store_module.store
    store_module.store = EventStore()

    try:
        monitor = MonitoringModule()
        doc = Document(content=b"hello", filename="test.pdf")
        result = StageResult(module="router", status="success", data={"route": "pdf"}, duration_ms=2.0)
        await monitor.on_event(doc, result)

        assert store_module.store.stats["total"] == 1
        assert store_module.store.stats["by_route"]["pdf"] == 1
    finally:
        store_module.store = original
