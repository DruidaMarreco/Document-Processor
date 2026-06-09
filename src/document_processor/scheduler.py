"""Background task: poll scheduled_jobs and dispatch due entries to the job queue."""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from document_processor import storage
from document_processor.models import Document, JobRecord

logger = logging.getLogger(__name__)

_POLL_INTERVAL = 60  # seconds


async def run_scheduler(job_queue: asyncio.Queue) -> None:
    logger.info("Scheduler started — polling every %ds", _POLL_INTERVAL)
    while True:
        try:
            now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            due = await storage.claim_due_scheduled_jobs(now)
            for sched in due:
                doc = await storage.get_document(sched["id"])
                if doc is None:
                    await storage.update_scheduled_job_status(sched["id"], "failed")
                    continue
                job = JobRecord(
                    status="queued",
                    filename=sched["filename"],
                    mimetype=sched["mimetype"],
                )
                doc_with_id = Document(
                    id=job.job_id,
                    filename=doc.filename,
                    mimetype=doc.mimetype,
                    content=doc.content,
                )
                await storage.create_job(job, doc_with_id)
                await storage.update_scheduled_job_status(sched["id"], "dispatched",
                                                          job_id=str(job.job_id))
                await job_queue.put(job.job_id)
                logger.info("Scheduled job %s dispatched as job %s", sched["id"], job.job_id)
        except asyncio.CancelledError:
            logger.info("Scheduler shutting down")
            break
        except Exception as exc:
            logger.exception("Scheduler error: %s", exc)
        try:
            await asyncio.sleep(_POLL_INTERVAL)
        except asyncio.CancelledError:
            break
