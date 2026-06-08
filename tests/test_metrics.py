"""Tests for Prometheus metrics endpoint."""
from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

from monitoring_module.api import app


@pytest.mark.asyncio
async def test_metrics_content_type():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get("/metrics")
    assert r.status_code == 200
    assert "text/plain" in r.headers["content-type"]


@pytest.mark.asyncio
async def test_metrics_contains_required_keys():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get("/metrics")
    text = r.text
    assert "doc_processor_documents_total" in text
    assert "doc_processor_success_total" in text
    assert "doc_processor_errors_total" in text
    assert "doc_processor_llm_calls_total" in text
    assert "doc_processor_avg_duration_ms" in text


@pytest.mark.asyncio
async def test_metrics_prometheus_format():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get("/metrics")
    lines = r.text.strip().splitlines()
    help_lines = [l for l in lines if l.startswith("# HELP")]
    type_lines = [l for l in lines if l.startswith("# TYPE")]
    assert len(help_lines) > 0
    assert len(type_lines) > 0
    assert len(help_lines) == len(type_lines)
