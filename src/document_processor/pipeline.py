import time
from collections.abc import Callable, Coroutine
from typing import Any

from loguru import logger

from document_processor.models import Document, PipelineResult, StageResult
from document_processor.modules.base import Module

Listener = Callable[[Document, StageResult], Coroutine[Any, Any, None]]


class Pipeline:
    def __init__(self, stages: list[Module], listeners: list[Listener] | None = None) -> None:
        self.stages = stages
        self._listeners: list[Listener] = listeners or []

    def add_listener(self, listener: Listener) -> None:
        self._listeners.append(listener)

    async def _notify(self, document: Document, result: StageResult) -> None:
        for listener in self._listeners:
            try:
                await listener(document, result)
            except Exception as exc:
                logger.warning("pipeline.listener.error", error=str(exc))

    async def run(self, document: Document, context: dict | None = None) -> PipelineResult:
        start = time.monotonic()
        context = dict(context) if context else {}
        stage_results: list[StageResult] = []
        failed = False

        logger.info(
            "pipeline.start",
            document_id=str(document.id),
            filename=document.filename,
            mimetype=document.mimetype,
            stages=[s.name for s in self.stages],
        )

        for module in self.stages:
            stage_start = time.monotonic()
            try:
                result = await module.process(document, context)
                context[module.name] = result.data
            except Exception as exc:
                result = StageResult(
                    module=module.name,
                    status="error",
                    errors=[str(exc)],
                    duration_ms=(time.monotonic() - stage_start) * 1000,
                )
                failed = True

            stage_results.append(result)
            logger.info(
                "pipeline.stage",
                module=module.name,
                status=result.status,
                duration_ms=round(result.duration_ms, 2),
                errors=result.errors or None,
            )
            await self._notify(document, result)

            if failed:
                for remaining in self.stages[len(stage_results):]:
                    skipped = StageResult(module=remaining.name, status="skipped", duration_ms=0.0)
                    stage_results.append(skipped)
                    await self._notify(document, skipped)
                break

        total_ms = (time.monotonic() - start) * 1000
        has_error = any(r.status == "error" for r in stage_results)
        overall = "failed" if failed else ("partial" if has_error else "success")

        result = PipelineResult(
            document_id=document.id,
            status=overall,
            stages=stage_results,
            output=context.get("generator", {}),
            total_duration_ms=round(total_ms, 2),
        )

        logger.info(
            "pipeline.done",
            document_id=str(document.id),
            status=overall,
            total_ms=round(total_ms, 2),
        )
        return result
