"""Fire-and-forget webhook delivery with HMAC signing and retry logic."""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
from datetime import datetime, timezone

import httpx

from document_processor.models import PipelineResult, WebhookConfig

logger = logging.getLogger(__name__)

_MAX_RETRIES = 3
_RETRY_DELAYS = (1.0, 2.0, 4.0)


def _sign(body: bytes, secret: str) -> str:
    return "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


async def _deliver_one(wh: WebhookConfig, event: str, payload: bytes) -> bool:
    headers: dict[str, str] = {
        "Content-Type": "application/json",
        "X-Webhook-Event": event,
        "X-Delivery-Timestamp": datetime.now(timezone.utc).isoformat(),
    }
    if wh.secret:
        headers["X-Signature-SHA256"] = _sign(payload, wh.secret)

    async with httpx.AsyncClient(timeout=10.0) as client:
        for attempt, delay in enumerate((*_RETRY_DELAYS,)):
            try:
                r = await client.post(wh.url, content=payload, headers=headers)
                if r.status_code < 500:
                    return True
                logger.warning("Webhook %s attempt %d got HTTP %d", wh.id, attempt + 1, r.status_code)
            except Exception as exc:
                logger.warning("Webhook %s attempt %d failed: %s", wh.id, attempt + 1, exc)
            if attempt < _MAX_RETRIES - 1:
                await asyncio.sleep(delay)
    return False


async def fire_webhooks(
    webhooks: list[WebhookConfig],
    event: str,
    result: PipelineResult,
) -> None:
    """Deliver event to all matching active webhooks concurrently."""
    matching = [wh for wh in webhooks if wh.active and event in wh.events]
    if not matching:
        return

    payload = json.dumps({
        "event": event,
        "document_id": str(result.document_id),
        "status": result.status,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "result": result.model_dump(mode="json"),
    }).encode()

    await asyncio.gather(*[_deliver_one(wh, event, payload) for wh in matching])
