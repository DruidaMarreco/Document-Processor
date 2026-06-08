"""Async SQLite persistence for Document, PipelineResult, WebhookConfig, and ApiKey objects."""
from __future__ import annotations

import csv
import hashlib
import io
import json
from pathlib import Path
from uuid import UUID

import aiosqlite

from document_processor.models import ApiKey, ApiKeyInfo, Document, JobRecord, PipelineResult, WebhookConfig

_DB_PATH = Path("data/results.db")

_DDL_RESULTS = """
    CREATE TABLE IF NOT EXISTS results (
        id              TEXT PRIMARY KEY,
        doc_type        TEXT,
        pipeline_status TEXT,
        filename        TEXT,
        data            TEXT NOT NULL,
        created_at      TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
    )
"""
_DDL_DOCUMENTS = """
    CREATE TABLE IF NOT EXISTS documents (
        id           TEXT PRIMARY KEY,
        filename     TEXT,
        mimetype     TEXT,
        content      BLOB NOT NULL,
        content_hash TEXT
    )
"""
_DDL_TAGS = """
    CREATE TABLE IF NOT EXISTS tags (
        result_id  TEXT NOT NULL,
        tag        TEXT NOT NULL,
        created_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
        PRIMARY KEY (result_id, tag)
    )
"""
_DDL_WEBHOOKS = """
    CREATE TABLE IF NOT EXISTS webhooks (
        id         TEXT PRIMARY KEY,
        url        TEXT NOT NULL,
        events     TEXT NOT NULL,
        secret     TEXT,
        active     INTEGER NOT NULL DEFAULT 1,
        created_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
    )
"""
_DDL_API_KEYS = """
    CREATE TABLE IF NOT EXISTS api_keys (
        key_hash   TEXT PRIMARY KEY,
        prefix     TEXT NOT NULL,
        name       TEXT NOT NULL,
        created_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
    )
"""
_DDL_JOBS = """
    CREATE TABLE IF NOT EXISTS jobs (
        job_id       TEXT PRIMARY KEY,
        status       TEXT NOT NULL DEFAULT 'queued',
        filename     TEXT,
        mimetype     TEXT,
        created_at   TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
        started_at   TEXT,
        completed_at TEXT,
        result_id    TEXT,
        error        TEXT
    )
"""
# Columns added after initial schema — migrated at startup
_MIGRATION_COLUMNS = [
    ("doc_type",        "TEXT"),
    ("pipeline_status", "TEXT"),
    ("filename",        "TEXT"),
]
_MIGRATION_DOCUMENTS_COLUMNS = [
    ("content_hash", "TEXT"),
]


def _db_path() -> Path:
    return _DB_PATH


def set_db_path(path: Path) -> None:
    global _DB_PATH
    _DB_PATH = path


def _ensure_dir() -> None:
    _db_path().parent.mkdir(parents=True, exist_ok=True)


async def init_db() -> None:
    """Create tables and migrate schema. Call from app lifespan."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_RESULTS)
        await db.execute(_DDL_DOCUMENTS)
        await db.execute(_DDL_WEBHOOKS)
        await db.execute(_DDL_API_KEYS)
        await db.execute(_DDL_JOBS)
        await db.execute(_DDL_TAGS)
        # Add columns that may not exist in older databases
        for col, col_type in _MIGRATION_COLUMNS:
            try:
                await db.execute(f"ALTER TABLE results ADD COLUMN {col} {col_type}")
            except Exception:
                pass  # column already exists
        for col, col_type in _MIGRATION_DOCUMENTS_COLUMNS:
            try:
                await db.execute(f"ALTER TABLE documents ADD COLUMN {col} {col_type}")
            except Exception:
                pass
        await db.commit()


def _extract_meta(result: PipelineResult) -> tuple[str | None, str, str | None]:
    """Pull (doc_type, pipeline_status, filename) out of a PipelineResult."""
    doc_type: str | None = None
    filename: str | None = None
    for stage in result.stages:
        if stage.module == "classifier":
            doc_type = stage.data.get("type")
        if stage.module == "extractor":
            filename = (stage.data.get("metadata") or {}).get("filename")
    return doc_type, result.status, filename


def _content_hash(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


async def find_duplicate(content: bytes) -> PipelineResult | None:
    """Return the most recent result for documents with identical content, or None."""
    h = _content_hash(content)
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_DOCUMENTS)
        await db.execute(_DDL_RESULTS)
        await db.commit()
        async with db.execute(
            """SELECT r.data FROM results r
               JOIN documents d ON d.id = r.id
               WHERE d.content_hash = ?
               ORDER BY r.created_at DESC LIMIT 1""",
            (h,),
        ) as cursor:
            row = await cursor.fetchone()
    return PipelineResult.model_validate_json(row[0]) if row else None


async def save_result(result: PipelineResult, document: Document | None = None) -> None:
    doc_type, pipeline_status, filename = _extract_meta(result)
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_RESULTS)
        await db.execute(_DDL_DOCUMENTS)
        await db.execute(
            """INSERT OR REPLACE INTO results (id, doc_type, pipeline_status, filename, data)
               VALUES (?, ?, ?, ?, ?)""",
            (str(result.document_id), doc_type, pipeline_status, filename,
             result.model_dump_json()),
        )
        if document is not None:
            h = _content_hash(document.content)
            await db.execute(
                """INSERT OR REPLACE INTO documents (id, filename, mimetype, content, content_hash)
                   VALUES (?, ?, ?, ?, ?)""",
                (str(result.document_id), document.filename,
                 document.mimetype, document.content, h),
            )
        await db.commit()


async def get_result(document_id: UUID) -> PipelineResult | None:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_RESULTS)
        await db.commit()
        async with db.execute(
            "SELECT data FROM results WHERE id = ?", (str(document_id),)
        ) as cursor:
            row = await cursor.fetchone()
    return PipelineResult.model_validate_json(row[0]) if row else None


async def get_document(document_id: UUID) -> Document | None:
    """Retrieve the original document bytes stored at upload time."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_DOCUMENTS)
        await db.commit()
        async with db.execute(
            "SELECT filename, mimetype, content FROM documents WHERE id = ?",
            (str(document_id),),
        ) as cursor:
            row = await cursor.fetchone()
    if row:
        return Document(filename=row[0], mimetype=row[1] or "application/octet-stream",
                        content=bytes(row[2]))
    return None


async def search_results(
    doc_type: str | None = None,
    status: str | None = None,
    filename: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> tuple[list[PipelineResult], int]:
    """Return (page, total_count) with optional filters."""
    conditions: list[str] = []
    params: list[object] = []
    if doc_type:
        conditions.append("doc_type = ?")
        params.append(doc_type)
    if status:
        conditions.append("pipeline_status = ?")
        params.append(status)
    if filename:
        conditions.append("filename LIKE ?")
        params.append(f"%{filename}%")

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_RESULTS)
        await db.commit()
        async with db.execute(
            f"SELECT COUNT(*) FROM results {where}", params
        ) as cursor:
            total: int = (await cursor.fetchone())[0]  # type: ignore[index]
        async with db.execute(
            f"SELECT data FROM results {where} ORDER BY created_at DESC LIMIT ? OFFSET ?",
            params + [limit, offset],
        ) as cursor:
            rows = await cursor.fetchall()

    return [PipelineResult.model_validate_json(r[0]) for r in rows], total


async def delete_result(document_id: UUID) -> None:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute("DELETE FROM results WHERE id = ?", (str(document_id),))
        await db.execute("DELETE FROM documents WHERE id = ?", (str(document_id),))
        await db.commit()


async def list_results(limit: int = 100) -> list[PipelineResult]:
    results, _ = await search_results(limit=limit)
    return results


async def count_results() -> int:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_RESULTS)
        await db.commit()
        async with db.execute("SELECT COUNT(*) FROM results") as cursor:
            row = await cursor.fetchone()
    return row[0] if row else 0


# ---------------------------------------------------------------------------
# Webhook persistence
# ---------------------------------------------------------------------------

async def save_webhook(wh: WebhookConfig) -> None:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_WEBHOOKS)
        await db.execute(
            """INSERT OR REPLACE INTO webhooks (id, url, events, secret, active)
               VALUES (?, ?, ?, ?, ?)""",
            (str(wh.id), wh.url, json.dumps(wh.events), wh.secret, int(wh.active)),
        )
        await db.commit()


async def list_webhooks(active_only: bool = False) -> list[WebhookConfig]:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_WEBHOOKS)
        await db.commit()
        where = "WHERE active = 1" if active_only else ""
        async with db.execute(
            f"SELECT id, url, events, secret, active, created_at FROM webhooks {where}"
        ) as cursor:
            rows = await cursor.fetchall()
    return [
        WebhookConfig(
            id=row[0], url=row[1], events=json.loads(row[2]),
            secret=row[3], active=bool(row[4]),
        )
        for row in rows
    ]


async def get_webhook(webhook_id: UUID) -> WebhookConfig | None:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_WEBHOOKS)
        await db.commit()
        async with db.execute(
            "SELECT id, url, events, secret, active FROM webhooks WHERE id = ?",
            (str(webhook_id),),
        ) as cursor:
            row = await cursor.fetchone()
    if not row:
        return None
    return WebhookConfig(
        id=row[0], url=row[1], events=json.loads(row[2]),
        secret=row[3], active=bool(row[4]),
    )


async def delete_webhook(webhook_id: UUID) -> bool:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_WEBHOOKS)
        cursor = await db.execute(
            "DELETE FROM webhooks WHERE id = ?", (str(webhook_id),)
        )
        await db.commit()
    return (cursor.rowcount or 0) > 0


# ---------------------------------------------------------------------------
# Aggregate statistics
# ---------------------------------------------------------------------------

async def get_stats() -> dict:
    """Return aggregate counts and averages across all stored results."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_RESULTS)
        await db.commit()

        async with db.execute("SELECT COUNT(*) FROM results") as cur:
            total: int = (await cur.fetchone())[0]  # type: ignore[index]

        async with db.execute(
            "SELECT pipeline_status, COUNT(*) FROM results GROUP BY pipeline_status"
        ) as cur:
            by_status = {row[0] or "unknown": row[1] for row in await cur.fetchall()}

        async with db.execute(
            "SELECT doc_type, COUNT(*) FROM results GROUP BY doc_type"
        ) as cur:
            by_doc_type = {row[0] or "unknown": row[1] for row in await cur.fetchall()}

        async with db.execute(
            "SELECT AVG(CAST(json_extract(data, '$.total_duration_ms') AS REAL)) FROM results"
        ) as cur:
            row = await cur.fetchone()
            avg_duration_ms: float = round(row[0] or 0.0, 2)

        async with db.execute(
            "SELECT COUNT(*) FROM results WHERE created_at >= strftime('%Y-%m-%dT%H:%M:%SZ', "
            "datetime('now', '-1 day'))"
        ) as cur:
            recent_24h: int = (await cur.fetchone())[0]  # type: ignore[index]

    return {
        "total": total,
        "by_status": by_status,
        "by_doc_type": by_doc_type,
        "avg_duration_ms": avg_duration_ms,
        "recent_24h": recent_24h,
    }


def result_to_csv(result: PipelineResult) -> str:
    """Flatten a PipelineResult into a two-row CSV (header + data)."""
    extractor_data: dict = {}
    classifier_data: dict = {}
    validator_data: dict = {}
    for stage in result.stages:
        if stage.module == "extractor":
            extractor_data = stage.data
        elif stage.module == "classifier":
            classifier_data = stage.data
        elif stage.module == "validator":
            validator_data = stage.data

    fields = extractor_data.get("fields", {})
    meta = extractor_data.get("metadata", {})

    row: dict = {
        "document_id": str(result.document_id),
        "filename": meta.get("filename", ""),
        "doc_type": classifier_data.get("type", ""),
        "confidence": classifier_data.get("confidence", ""),
        "method": classifier_data.get("method", ""),
        "pipeline_status": result.status,
        "valid": validator_data.get("valid", ""),
        "violations": "; ".join(validator_data.get("violations", [])),
        "warnings": "; ".join(validator_data.get("warnings", [])),
        "total_duration_ms": result.total_duration_ms,
    }
    for field_name, values in fields.items():
        if isinstance(values, list):
            row[field_name] = "; ".join(str(v) for v in values)
        else:
            row[field_name] = str(values)

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(row.keys()))
    writer.writeheader()
    writer.writerow(row)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# API key persistence
# ---------------------------------------------------------------------------

def _hash_key(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()


async def save_api_key(api_key: ApiKey) -> None:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_API_KEYS)
        await db.execute(
            "INSERT OR REPLACE INTO api_keys (key_hash, prefix, name) VALUES (?, ?, ?)",
            (_hash_key(api_key.key), api_key.key[:8], api_key.name),
        )
        await db.commit()


async def verify_api_key(key: str) -> bool:
    """Return True if the key exists in the database."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_API_KEYS)
        await db.commit()
        async with db.execute(
            "SELECT 1 FROM api_keys WHERE key_hash = ?", (_hash_key(key),)
        ) as cursor:
            return await cursor.fetchone() is not None


async def list_api_keys() -> list[ApiKeyInfo]:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_API_KEYS)
        await db.commit()
        async with db.execute(
            "SELECT prefix, name, created_at FROM api_keys ORDER BY created_at DESC"
        ) as cursor:
            rows = await cursor.fetchall()
    return [ApiKeyInfo(prefix=r[0], name=r[1], created_at=r[2]) for r in rows]


async def delete_api_key_by_prefix(prefix: str) -> bool:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_API_KEYS)
        cursor = await db.execute(
            "DELETE FROM api_keys WHERE prefix = ?", (prefix,)
        )
        await db.commit()
    return (cursor.rowcount or 0) > 0


# ---------------------------------------------------------------------------
# Async job queue persistence
# ---------------------------------------------------------------------------

def _row_to_job(row: tuple) -> JobRecord:
    job_id, status, filename, mimetype, created_at, started_at, completed_at, result_id, error = row
    return JobRecord(
        job_id=job_id,
        status=status,
        filename=filename,
        mimetype=mimetype or "application/octet-stream",
        created_at=created_at,
        started_at=started_at,
        completed_at=completed_at,
        result_id=result_id,
        error=error,
    )


async def create_job(job: JobRecord, document: Document) -> None:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_JOBS)
        await db.execute(_DDL_DOCUMENTS)
        await db.execute(
            """INSERT INTO jobs (job_id, status, filename, mimetype)
               VALUES (?, ?, ?, ?)""",
            (str(job.job_id), job.status, job.filename, job.mimetype),
        )
        await db.execute(
            """INSERT OR REPLACE INTO documents (id, filename, mimetype, content, content_hash)
               VALUES (?, ?, ?, ?, ?)""",
            (str(job.job_id), document.filename, document.mimetype, document.content,
             _content_hash(document.content)),
        )
        await db.commit()


async def get_job(job_id: UUID) -> JobRecord | None:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_JOBS)
        await db.commit()
        async with db.execute(
            "SELECT job_id, status, filename, mimetype, created_at, started_at, "
            "completed_at, result_id, error FROM jobs WHERE job_id = ?",
            (str(job_id),),
        ) as cursor:
            row = await cursor.fetchone()
    return _row_to_job(row) if row else None


async def update_job(
    job_id: UUID,
    status: str,
    started_at: str | None = None,
    completed_at: str | None = None,
    result_id: UUID | None = None,
    error: str | None = None,
) -> None:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_JOBS)
        await db.execute(
            """UPDATE jobs SET status=?, started_at=COALESCE(?, started_at),
               completed_at=COALESCE(?, completed_at),
               result_id=COALESCE(?, result_id),
               error=COALESCE(?, error)
               WHERE job_id=?""",
            (status, started_at, completed_at,
             str(result_id) if result_id else None,
             error, str(job_id)),
        )
        await db.commit()


async def list_jobs(limit: int = 50, offset: int = 0) -> list[JobRecord]:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_JOBS)
        await db.commit()
        async with db.execute(
            "SELECT job_id, status, filename, mimetype, created_at, started_at, "
            "completed_at, result_id, error FROM jobs "
            "ORDER BY created_at DESC LIMIT ? OFFSET ?",
            (limit, offset),
        ) as cursor:
            rows = await cursor.fetchall()
    return [_row_to_job(r) for r in rows]


async def get_job_document(job_id: UUID) -> Document | None:
    """Retrieve document bytes stored when the job was queued."""
    return await get_document(job_id)


# ---------------------------------------------------------------------------
# Result tagging
# ---------------------------------------------------------------------------

async def add_tags(result_id: UUID, tags: list[str]) -> None:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_TAGS)
        for tag in tags:
            await db.execute(
                "INSERT OR IGNORE INTO tags (result_id, tag) VALUES (?, ?)",
                (str(result_id), tag.strip().lower()),
            )
        await db.commit()


async def get_tags(result_id: UUID) -> list[str]:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_TAGS)
        await db.commit()
        async with db.execute(
            "SELECT tag FROM tags WHERE result_id = ? ORDER BY tag",
            (str(result_id),),
        ) as cursor:
            rows = await cursor.fetchall()
    return [r[0] for r in rows]


async def remove_tag(result_id: UUID, tag: str) -> bool:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_TAGS)
        cursor = await db.execute(
            "DELETE FROM tags WHERE result_id = ? AND tag = ?",
            (str(result_id), tag.strip().lower()),
        )
        await db.commit()
    return (cursor.rowcount or 0) > 0


async def search_results_by_tag(
    tag: str,
    limit: int = 50,
    offset: int = 0,
) -> tuple[list[PipelineResult], int]:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_RESULTS)
        await db.execute(_DDL_TAGS)
        await db.commit()
        async with db.execute(
            "SELECT COUNT(*) FROM results r JOIN tags t ON t.result_id = r.id WHERE t.tag = ?",
            (tag.strip().lower(),),
        ) as cur:
            total: int = (await cur.fetchone())[0]  # type: ignore[index]
        async with db.execute(
            """SELECT r.data FROM results r
               JOIN tags t ON t.result_id = r.id
               WHERE t.tag = ?
               ORDER BY r.created_at DESC LIMIT ? OFFSET ?""",
            (tag.strip().lower(), limit, offset),
        ) as cur:
            rows = await cur.fetchall()
    return [PipelineResult.model_validate_json(r[0]) for r in rows], total
