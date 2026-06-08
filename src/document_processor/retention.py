"""Periodic result-retention cleanup task."""
from __future__ import annotations

import asyncio
import logging

from document_processor import storage

logger = logging.getLogger(__name__)

_24H = 86_400


async def run_retention(retention_days: int) -> None:
    """Background task: purge results older than retention_days every 24 h."""
    logger.info("Retention policy active: deleting results older than %d days", retention_days)
    while True:
        try:
            deleted = await storage.cleanup_old_results(retention_days)
            if deleted:
                logger.info("Retention cleanup: deleted %d old results", deleted)
        except asyncio.CancelledError:
            logger.info("Retention task shutting down")
            break
        except Exception as exc:
            logger.exception("Retention cleanup error: %s", exc)
        try:
            await asyncio.sleep(_24H)
        except asyncio.CancelledError:
            break
