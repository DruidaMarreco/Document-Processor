from __future__ import annotations

import monitoring_module.store as _store_module
from document_processor.models import Document, StageResult
from monitoring_module.store import PipelineEvent


class MonitoringModule:
    """
    Cross-cutting listener — NOT a pipeline stage.
    Registered in registry.py via pipeline.add_listener(monitor.on_event).
    """

    name = "monitoring"

    async def on_event(self, document: Document, result: StageResult) -> None:
        event = PipelineEvent(
            document_id=str(document.id),
            module=result.module,
            status=result.status,
            data=result.data,
            errors=result.errors,
            duration_ms=result.duration_ms,
        )
        await _store_module.store.record(event)

    async def health_check(self) -> bool:
        return True
