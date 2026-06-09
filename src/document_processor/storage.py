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
        created_at      TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
        pinned          INTEGER NOT NULL DEFAULT 0
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
_DDL_NOTES = """
    CREATE TABLE IF NOT EXISTS notes (
        result_id  TEXT PRIMARY KEY,
        note       TEXT NOT NULL,
        updated_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
    )
"""
_DDL_AUDIT = """
    CREATE TABLE IF NOT EXISTS audit_log (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        result_id  TEXT NOT NULL,
        action     TEXT NOT NULL,
        detail     TEXT,
        created_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
    )
"""
_DDL_SCHEDULED = """
    CREATE TABLE IF NOT EXISTS scheduled_jobs (
        id           TEXT PRIMARY KEY,
        run_at       TEXT NOT NULL,
        filename     TEXT,
        mimetype     TEXT NOT NULL DEFAULT 'application/octet-stream',
        config       TEXT,
        status       TEXT NOT NULL DEFAULT 'pending',
        job_id       TEXT,
        created_at   TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
    )
"""
_DDL_TEMPLATES = """
    CREATE TABLE IF NOT EXISTS processing_templates (
        name        TEXT PRIMARY KEY,
        config      TEXT NOT NULL,
        description TEXT,
        created_at  TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
        updated_at  TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
    )
"""
_DDL_COLLECTIONS = """
    CREATE TABLE IF NOT EXISTS collections (
        id          TEXT PRIMARY KEY,
        name        TEXT NOT NULL,
        description TEXT,
        created_at  TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
    )
"""
_DDL_COLLECTION_MEMBERS = """
    CREATE TABLE IF NOT EXISTS collection_members (
        collection_id TEXT NOT NULL,
        result_id     TEXT NOT NULL,
        added_at      TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
        PRIMARY KEY (collection_id, result_id)
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
    ("pinned",          "INTEGER NOT NULL DEFAULT 0"),
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
        await db.execute(_DDL_NOTES)
        await db.execute(_DDL_AUDIT)
        await db.execute(_DDL_SCHEDULED)
        await db.execute(_DDL_TEMPLATES)
        await db.execute(_DDL_COLLECTIONS)
        await db.execute(_DDL_COLLECTION_MEMBERS)
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


_SORT_COLUMNS = {"created_at", "doc_type", "filename", "pipeline_status"}


async def search_results(
    doc_type: str | None = None,
    status: str | None = None,
    filename: str | None = None,
    q: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    pinned: bool | None = None,
    sort_by: str = "created_at",
    sort_order: str = "desc",
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
    if q:
        conditions.append("data LIKE ?")
        params.append(f"%{q}%")
    if date_from:
        conditions.append("created_at >= ?")
        params.append(date_from)
    if date_to:
        conditions.append("created_at <= ?")
        params.append(date_to)
    if pinned is not None:
        conditions.append("pinned = ?")
        params.append(1 if pinned else 0)

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    col = sort_by if sort_by in _SORT_COLUMNS else "created_at"
    order = "ASC" if sort_order.lower() == "asc" else "DESC"
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_RESULTS)
        await db.commit()
        async with db.execute(
            f"SELECT COUNT(*) FROM results {where}", params
        ) as cursor:
            total: int = (await cursor.fetchone())[0]  # type: ignore[index]
        async with db.execute(
            f"SELECT data FROM results {where} ORDER BY {col} {order} LIMIT ? OFFSET ?",
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


async def bulk_delete_results(document_ids: list[UUID]) -> int:
    """Delete multiple results (+ documents, tags, notes). Returns count actually deleted."""
    if not document_ids:
        return 0
    ids = [str(d) for d in document_ids]
    placeholders = ",".join("?" * len(ids))
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_RESULTS)
        await db.execute(_DDL_DOCUMENTS)
        await db.execute(_DDL_TAGS)
        await db.execute(_DDL_NOTES)
        await db.commit()
        async with db.execute(
            f"SELECT COUNT(*) FROM results WHERE id IN ({placeholders})", ids
        ) as cur:
            found: int = (await cur.fetchone())[0]  # type: ignore[index]
        for table in ("tags", "notes"):
            await db.execute(f"DELETE FROM {table} WHERE result_id IN ({placeholders})", ids)
        for table in ("documents", "results"):
            await db.execute(f"DELETE FROM {table} WHERE id IN ({placeholders})", ids)
        await db.commit()
    return found


async def bulk_add_tags(document_ids: list[UUID], tags: list[str]) -> int:
    """Add tags to multiple results. Skips IDs that don't exist. Returns count of results updated."""
    if not document_ids or not tags:
        return 0
    ids = [str(d) for d in document_ids]
    placeholders = ",".join("?" * len(ids))
    normalised = [t.strip().lower() for t in tags if t.strip()]
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_RESULTS)
        await db.execute(_DDL_TAGS)
        await db.commit()
        async with db.execute(
            f"SELECT id FROM results WHERE id IN ({placeholders})", ids
        ) as cur:
            existing_ids = [r[0] for r in await cur.fetchall()]
        for result_id in existing_ids:
            for tag in normalised:
                await db.execute(
                    "INSERT OR IGNORE INTO tags (result_id, tag) VALUES (?, ?)",
                    (result_id, tag),
                )
        await db.commit()
    return len(existing_ids)


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


# ---------------------------------------------------------------------------
# Retention / cleanup
# ---------------------------------------------------------------------------

async def cleanup_old_results(older_than_days: int) -> int:
    """Delete results (+ documents, tags, notes) older than N days. Returns count deleted."""
    if older_than_days <= 0:
        return 0
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_RESULTS)
        await db.execute(_DDL_DOCUMENTS)
        await db.execute(_DDL_TAGS)
        await db.execute(_DDL_NOTES)
        await db.commit()
        cutoff = f"datetime('now', '-{int(older_than_days)} days')"
        async with db.execute(
            f"SELECT id FROM results WHERE created_at < strftime('%Y-%m-%dT%H:%M:%SZ', {cutoff})"
            " AND pinned = 0"
        ) as cur:
            ids = [r[0] for r in await cur.fetchall()]
        if not ids:
            return 0
        placeholders = ",".join("?" * len(ids))
        for table in ("tags", "notes", "documents", "results"):
            col = "result_id" if table in ("tags", "notes") else "id"
            await db.execute(f"DELETE FROM {table} WHERE {col} IN ({placeholders})", ids)
        await db.commit()
    return len(ids)


# ---------------------------------------------------------------------------
# Document notes
# ---------------------------------------------------------------------------

async def set_note(result_id: UUID, note: str) -> None:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_NOTES)
        await db.execute(
            """INSERT INTO notes (result_id, note, updated_at)
               VALUES (?, ?, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
               ON CONFLICT(result_id) DO UPDATE SET
                   note = excluded.note,
                   updated_at = excluded.updated_at""",
            (str(result_id), note),
        )
        await db.commit()


async def get_note(result_id: UUID) -> dict | None:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_NOTES)
        await db.commit()
        async with db.execute(
            "SELECT note, updated_at FROM notes WHERE result_id = ?", (str(result_id),)
        ) as cur:
            row = await cur.fetchone()
    return {"note": row[0], "updated_at": row[1]} if row else None


async def delete_note(result_id: UUID) -> bool:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_NOTES)
        cursor = await db.execute(
            "DELETE FROM notes WHERE result_id = ?", (str(result_id),)
        )
        await db.commit()
    return (cursor.rowcount or 0) > 0


# ---------------------------------------------------------------------------
# Result pinning
# ---------------------------------------------------------------------------

async def set_pinned(result_id: UUID, pinned: bool) -> bool:
    """Pin or unpin a result. Returns False if the result does not exist."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_RESULTS)
        cursor = await db.execute(
            "UPDATE results SET pinned = ? WHERE id = ?",
            (1 if pinned else 0, str(result_id)),
        )
        await db.commit()
    return (cursor.rowcount or 0) > 0


async def is_pinned(result_id: UUID) -> bool | None:
    """Return pin state, or None if result does not exist."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_RESULTS)
        await db.commit()
        async with db.execute(
            "SELECT pinned FROM results WHERE id = ?", (str(result_id),)
        ) as cur:
            row = await cur.fetchone()
    return bool(row[0]) if row is not None else None


# ---------------------------------------------------------------------------
# Audit log
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Scheduled processing
# ---------------------------------------------------------------------------

async def create_scheduled_job(
    scheduled_id: str,
    run_at: str,
    document: "Document",
    config: dict | None = None,
) -> dict:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_SCHEDULED)
        await db.execute(_DDL_DOCUMENTS)
        await db.execute(
            """INSERT INTO scheduled_jobs (id, run_at, filename, mimetype, config)
               VALUES (?, ?, ?, ?, ?)""",
            (scheduled_id, run_at, document.filename, document.mimetype,
             json.dumps(config) if config else None),
        )
        await db.execute(
            """INSERT OR REPLACE INTO documents (id, filename, mimetype, content, content_hash)
               VALUES (?, ?, ?, ?, ?)""",
            (scheduled_id, document.filename, document.mimetype, document.content,
             _content_hash(document.content)),
        )
        await db.commit()
    return {"id": scheduled_id, "run_at": run_at, "status": "pending",
            "filename": document.filename}


async def list_scheduled_jobs(status: str | None = None) -> list[dict]:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_SCHEDULED)
        await db.commit()
        where = "WHERE status = ?" if status else ""
        params = [status] if status else []
        async with db.execute(
            f"SELECT id, run_at, filename, mimetype, config, status, job_id, created_at "
            f"FROM scheduled_jobs {where} ORDER BY run_at ASC",
            params,
        ) as cur:
            rows = await cur.fetchall()
    return [
        {"id": r[0], "run_at": r[1], "filename": r[2], "mimetype": r[3],
         "config": json.loads(r[4]) if r[4] else None, "status": r[5],
         "job_id": r[6], "created_at": r[7]}
        for r in rows
    ]


async def get_scheduled_job(scheduled_id: str) -> dict | None:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_SCHEDULED)
        await db.commit()
        async with db.execute(
            "SELECT id, run_at, filename, mimetype, config, status, job_id, created_at "
            "FROM scheduled_jobs WHERE id = ?",
            (scheduled_id,),
        ) as cur:
            row = await cur.fetchone()
    if not row:
        return None
    return {"id": row[0], "run_at": row[1], "filename": row[2], "mimetype": row[3],
            "config": json.loads(row[4]) if row[4] else None, "status": row[5],
            "job_id": row[6], "created_at": row[7]}


async def delete_scheduled_job(scheduled_id: str) -> bool:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_SCHEDULED)
        cursor = await db.execute(
            "DELETE FROM scheduled_jobs WHERE id = ? AND status = 'pending'",
            (scheduled_id,),
        )
        await db.commit()
    return (cursor.rowcount or 0) > 0


async def claim_due_scheduled_jobs(now: str) -> list[dict]:
    """Atomically mark due pending jobs as 'dispatched' and return them."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_SCHEDULED)
        await db.commit()
        async with db.execute(
            "SELECT id FROM scheduled_jobs WHERE status = 'pending' AND run_at <= ?",
            (now,),
        ) as cur:
            ids = [r[0] for r in await cur.fetchall()]
        if not ids:
            return []
        placeholders = ",".join("?" * len(ids))
        await db.execute(
            f"UPDATE scheduled_jobs SET status = 'dispatched' WHERE id IN ({placeholders})",
            ids,
        )
        await db.commit()
        async with db.execute(
            f"SELECT id, run_at, filename, mimetype, config, status, job_id, created_at "
            f"FROM scheduled_jobs WHERE id IN ({placeholders})",
            ids,
        ) as cur:
            rows = await cur.fetchall()
    return [
        {"id": r[0], "run_at": r[1], "filename": r[2], "mimetype": r[3],
         "config": json.loads(r[4]) if r[4] else None, "status": r[5],
         "job_id": r[6], "created_at": r[7]}
        for r in rows
    ]


async def update_scheduled_job_status(scheduled_id: str, status: str, job_id: str | None = None) -> None:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_SCHEDULED)
        await db.execute(
            "UPDATE scheduled_jobs SET status = ?, job_id = COALESCE(?, job_id) WHERE id = ?",
            (status, job_id, scheduled_id),
        )
        await db.commit()


# ---------------------------------------------------------------------------
# Processing templates
# ---------------------------------------------------------------------------

async def save_template(name: str, config: dict, description: str | None = None) -> dict:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_TEMPLATES)
        await db.execute(
            """INSERT INTO processing_templates (name, config, description, updated_at)
               VALUES (?, ?, ?, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
               ON CONFLICT(name) DO UPDATE SET
                   config = excluded.config,
                   description = COALESCE(excluded.description, description),
                   updated_at = excluded.updated_at""",
            (name, json.dumps(config), description),
        )
        await db.commit()
        async with db.execute(
            "SELECT name, config, description, created_at, updated_at "
            "FROM processing_templates WHERE name = ?", (name,)
        ) as cur:
            row = await cur.fetchone()
    return {"name": row[0], "config": json.loads(row[1]), "description": row[2],
            "created_at": row[3], "updated_at": row[4]}


async def get_template(name: str) -> dict | None:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_TEMPLATES)
        await db.commit()
        async with db.execute(
            "SELECT name, config, description, created_at, updated_at "
            "FROM processing_templates WHERE name = ?", (name,)
        ) as cur:
            row = await cur.fetchone()
    return {"name": row[0], "config": json.loads(row[1]), "description": row[2],
            "created_at": row[3], "updated_at": row[4]} if row else None


async def list_templates() -> list[dict]:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_TEMPLATES)
        await db.commit()
        async with db.execute(
            "SELECT name, config, description, created_at, updated_at "
            "FROM processing_templates ORDER BY name"
        ) as cur:
            rows = await cur.fetchall()
    return [{"name": r[0], "config": json.loads(r[1]), "description": r[2],
             "created_at": r[3], "updated_at": r[4]} for r in rows]


async def delete_template(name: str) -> bool:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_TEMPLATES)
        cursor = await db.execute(
            "DELETE FROM processing_templates WHERE name = ?", (name,)
        )
        await db.commit()
    return (cursor.rowcount or 0) > 0


# ---------------------------------------------------------------------------
# Collections
# ---------------------------------------------------------------------------

async def create_collection(name: str, description: str | None = None) -> dict:
    from uuid import uuid4 as _uuid4
    col_id = str(_uuid4())
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_COLLECTIONS)
        await db.execute(
            "INSERT INTO collections (id, name, description) VALUES (?, ?, ?)",
            (col_id, name, description),
        )
        await db.commit()
        async with db.execute(
            "SELECT id, name, description, created_at FROM collections WHERE id = ?", (col_id,)
        ) as cur:
            row = await cur.fetchone()
    return {"id": row[0], "name": row[1], "description": row[2], "created_at": row[3]}


async def get_collection(collection_id: str) -> dict | None:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_COLLECTIONS)
        await db.commit()
        async with db.execute(
            "SELECT id, name, description, created_at FROM collections WHERE id = ?",
            (collection_id,),
        ) as cur:
            row = await cur.fetchone()
    return {"id": row[0], "name": row[1], "description": row[2], "created_at": row[3]} if row else None


async def list_collections() -> list[dict]:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_COLLECTIONS)
        await db.commit()
        async with db.execute(
            "SELECT id, name, description, created_at FROM collections ORDER BY created_at DESC"
        ) as cur:
            rows = await cur.fetchall()
    return [{"id": r[0], "name": r[1], "description": r[2], "created_at": r[3]} for r in rows]


async def delete_collection(collection_id: str) -> bool:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_COLLECTIONS)
        await db.execute(_DDL_COLLECTION_MEMBERS)
        await db.execute("DELETE FROM collection_members WHERE collection_id = ?", (collection_id,))
        cursor = await db.execute("DELETE FROM collections WHERE id = ?", (collection_id,))
        await db.commit()
    return (cursor.rowcount or 0) > 0


async def add_to_collection(collection_id: str, result_ids: list[UUID]) -> int:
    """Add results to a collection. Returns count of rows actually inserted."""
    if not result_ids:
        return 0
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_RESULTS)
        await db.execute(_DDL_COLLECTIONS)
        await db.execute(_DDL_COLLECTION_MEMBERS)
        await db.commit()
        inserted = 0
        for rid in result_ids:
            cursor = await db.execute(
                "INSERT OR IGNORE INTO collection_members (collection_id, result_id) VALUES (?, ?)",
                (collection_id, str(rid)),
            )
            inserted += cursor.rowcount or 0
        await db.commit()
    return inserted


async def remove_from_collection(collection_id: str, result_id: UUID) -> bool:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_COLLECTION_MEMBERS)
        cursor = await db.execute(
            "DELETE FROM collection_members WHERE collection_id = ? AND result_id = ?",
            (collection_id, str(result_id)),
        )
        await db.commit()
    return (cursor.rowcount or 0) > 0


async def list_collection_results(
    collection_id: str, limit: int = 50, offset: int = 0
) -> tuple[list[PipelineResult], int]:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_RESULTS)
        await db.execute(_DDL_COLLECTION_MEMBERS)
        await db.commit()
        async with db.execute(
            "SELECT COUNT(*) FROM collection_members WHERE collection_id = ?", (collection_id,)
        ) as cur:
            total: int = (await cur.fetchone())[0]  # type: ignore[index]
        async with db.execute(
            """SELECT r.data FROM results r
               JOIN collection_members cm ON cm.result_id = r.id
               WHERE cm.collection_id = ?
               ORDER BY cm.added_at DESC LIMIT ? OFFSET ?""",
            (collection_id, limit, offset),
        ) as cur:
            rows = await cur.fetchall()
    return [PipelineResult.model_validate_json(r[0]) for r in rows], total


async def append_audit(result_id: UUID | str, action: str, detail: str | None = None) -> None:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_AUDIT)
        await db.execute(
            "INSERT INTO audit_log (result_id, action, detail) VALUES (?, ?, ?)",
            (str(result_id), action, detail),
        )
        await db.commit()


async def get_audit_log(result_id: UUID, limit: int = 100) -> list[dict]:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_AUDIT)
        await db.commit()
        async with db.execute(
            "SELECT id, action, detail, created_at FROM audit_log "
            "WHERE result_id = ? ORDER BY id ASC LIMIT ?",
            (str(result_id), limit),
        ) as cur:
            rows = await cur.fetchall()
    return [
        {"id": r[0], "action": r[1], "detail": r[2], "created_at": r[3]}
        for r in rows
    ]
