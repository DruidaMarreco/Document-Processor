"""Background asyncio worker that processes queued jobs."""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from uuid import UUID

from document_processor import registry, storage
from document_processor.webhook_delivery import fire_webhooks

logger = logging.getLogger(__name__)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


async def _process_job(job_id: UUID) -> None:
    await storage.update_job(job_id, status="processing", started_at=_now())
    document = await storage.get_job_document(job_id)
    if not document:
        await storage.update_job(job_id, status="failed", completed_at=_now(),
                                 error="Document bytes not found")
        return
    try:
        pipeline = registry.build_pipeline()
        result = await pipeline.run(document)
        await storage.save_result(result, document=document)
        await storage.update_job(
            job_id, status="done", completed_at=_now(), result_id=result.document_id
        )
        event = "document.processed" if result.status != "failed" else "document.failed"
        webhooks = await storage.list_webhooks(active_only=True)
        await fire_webhooks(webhooks, event, result)
    except Exception as exc:
        logger.exception("Job %s failed: %s", job_id, exc)
        await storage.update_job(job_id, status="failed", completed_at=_now(),
                                 error=str(exc))


async def run_worker(queue: asyncio.Queue) -> None:
    """Long-running coroutine — run as an asyncio.Task from app lifespan."""
    logger.info("Job worker started")
    while True:
        try:
            job_id: UUID = await queue.get()
            await _process_job(job_id)
        except asyncio.CancelledError:
            logger.info("Job worker shutting down")
            break
        except Exception as exc:
            logger.exception("Worker loop error: %s", exc)
        finally:
            try:
                queue.task_done()
            except Exception:
                pass
