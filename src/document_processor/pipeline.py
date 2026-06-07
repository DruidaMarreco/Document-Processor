import time

from loguru import logger

from document_processor.models import Document, PipelineResult, StageResult
from document_processor.modules.base import Module


class Pipeline:
    def __init__(self, stages: list[Module]) -> None:
        self.stages = stages

    async def run(self, document: Document) -> PipelineResult:
        start = time.monotonic()
        context: dict = {}
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

            if failed:
                for remaining in self.stages[len(stage_results):]:
                    stage_results.append(
                        StageResult(module=remaining.name, status="skipped", duration_ms=0.0)
                    )
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
