"""Tests for SQLite persistence layer."""
from __future__ import annotations

import tempfile
from pathlib import Path
from uuid import uuid4

import pytest

from document_processor import storage
from document_processor.models import PipelineResult, StageResult


def _make_result(status: str = "success") -> PipelineResult:
    return PipelineResult(
        document_id=uuid4(),
        status=status,
        stages=[
            StageResult(module="router", status="success", duration_ms=1.0),
        ],
        output={"summary": "test"},
        total_duration_ms=5.0,
    )


@pytest.fixture(autouse=True)
def tmp_db(tmp_path: Path):
    storage.set_db_path(tmp_path / "test.db")
    yield
    storage.set_db_path(Path("data/results.db"))


@pytest.mark.asyncio
async def test_save_and_retrieve():
    await storage.init_db()
    r = _make_result()
    await storage.save_result(r)
    retrieved = await storage.get_result(r.document_id)
    assert retrieved is not None
    assert retrieved.document_id == r.document_id
    assert retrieved.status == r.status


@pytest.mark.asyncio
async def test_get_missing_returns_none():
    await storage.init_db()
    result = await storage.get_result(uuid4())
    assert result is None


@pytest.mark.asyncio
async def test_list_results_empty():
    await storage.init_db()
    results = await storage.list_results()
    assert results == []


@pytest.mark.asyncio
async def test_list_results_returns_saved():
    await storage.init_db()
    r1, r2 = _make_result(), _make_result("partial")
    await storage.save_result(r1)
    await storage.save_result(r2)
    results = await storage.list_results()
    assert len(results) == 2


@pytest.mark.asyncio
async def test_save_replaces_existing():
    await storage.init_db()
    r = _make_result()
    await storage.save_result(r)

    updated = PipelineResult(
        document_id=r.document_id,
        status="failed",
        stages=r.stages,
        output={},
        total_duration_ms=99.0,
    )
    await storage.save_result(updated)

    retrieved = await storage.get_result(r.document_id)
    assert retrieved is not None
    assert retrieved.status == "failed"
    count = await storage.count_results()
    assert count == 1


@pytest.mark.asyncio
async def test_count_results():
    await storage.init_db()
    assert await storage.count_results() == 0
    await storage.save_result(_make_result())
    await storage.save_result(_make_result())
    assert await storage.count_results() == 2


@pytest.mark.asyncio
async def test_list_respects_limit():
    await storage.init_db()
    for _ in range(5):
        await storage.save_result(_make_result())
    results = await storage.list_results(limit=3)
    assert len(results) == 3
