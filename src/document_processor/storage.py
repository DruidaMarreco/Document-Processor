"""Async SQLite persistence for PipelineResult objects."""
from __future__ import annotations

from pathlib import Path
from uuid import UUID

import aiosqlite

from document_processor.models import PipelineResult

_DB_PATH = Path("data/results.db")
_DDL = """
    CREATE TABLE IF NOT EXISTS results (
        id          TEXT PRIMARY KEY,
        data        TEXT NOT NULL,
        created_at  TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
    )
"""


def _db_path() -> Path:
    return _DB_PATH


def set_db_path(path: Path) -> None:
    global _DB_PATH
    _DB_PATH = path


def _ensure_dir() -> None:
    _db_path().parent.mkdir(parents=True, exist_ok=True)


async def init_db() -> None:
    """Explicit init — call from app lifespan for eager setup."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL)
        await db.commit()


async def save_result(result: PipelineResult) -> None:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL)
        await db.execute(
            "INSERT OR REPLACE INTO results (id, data) VALUES (?, ?)",
            (str(result.document_id), result.model_dump_json()),
        )
        await db.commit()


async def get_result(document_id: UUID) -> PipelineResult | None:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL)
        await db.commit()
        async with db.execute(
            "SELECT data FROM results WHERE id = ?", (str(document_id),)
        ) as cursor:
            row = await cursor.fetchone()
    if row:
        return PipelineResult.model_validate_json(row[0])
    return None


async def list_results(limit: int = 100) -> list[PipelineResult]:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL)
        await db.commit()
        async with db.execute(
            "SELECT data FROM results ORDER BY created_at DESC LIMIT ?", (limit,)
        ) as cursor:
            rows = await cursor.fetchall()
    return [PipelineResult.model_validate_json(row[0]) for row in rows]


async def count_results() -> int:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL)
        await db.commit()
        async with db.execute("SELECT COUNT(*) FROM results") as cursor:
            row = await cursor.fetchone()
    return row[0] if row else 0
