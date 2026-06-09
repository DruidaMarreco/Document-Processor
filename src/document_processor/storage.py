"""Async SQLite persistence for Document, PipelineResult, WebhookConfig, and ApiKey objects."""
from __future__ import annotations

import csv
import hashlib
import io
import json
import zipfile
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
        pinned          INTEGER NOT NULL DEFAULT 0,
        workflow_status TEXT,
        locked          INTEGER NOT NULL DEFAULT 0,
        starred         INTEGER NOT NULL DEFAULT 0,
        priority        TEXT,
        expires_at      TEXT,
        rating          INTEGER
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
_DDL_SNAPSHOTS = """
    CREATE TABLE IF NOT EXISTS result_snapshots (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        result_id  TEXT NOT NULL,
        label      TEXT,
        data       TEXT NOT NULL,
        created_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
    )
"""
_DDL_RELATIONS = """
    CREATE TABLE IF NOT EXISTS result_relations (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        source_id   TEXT NOT NULL,
        target_id   TEXT NOT NULL,
        relation    TEXT NOT NULL,
        created_at  TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
        UNIQUE (source_id, target_id, relation)
    )
"""
_DDL_METADATA = """
    CREATE TABLE IF NOT EXISTS result_metadata (
        result_id  TEXT NOT NULL,
        key        TEXT NOT NULL,
        value      TEXT NOT NULL,
        updated_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
        PRIMARY KEY (result_id, key)
    )
"""
_DDL_RESULT_COSTS = """
    CREATE TABLE IF NOT EXISTS result_costs (
        result_id          TEXT PRIMARY KEY,
        input_tokens       INTEGER NOT NULL DEFAULT 0,
        output_tokens      INTEGER NOT NULL DEFAULT 0,
        model              TEXT,
        estimated_cost_usd REAL,
        created_at         TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
    )
"""
_DDL_ATTACHMENTS = """
    CREATE TABLE IF NOT EXISTS result_attachments (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        result_id   TEXT NOT NULL,
        filename    TEXT NOT NULL,
        mimetype    TEXT NOT NULL DEFAULT 'application/octet-stream',
        size        INTEGER NOT NULL DEFAULT 0,
        content     BLOB NOT NULL,
        created_at  TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
    )
"""
_DDL_BOOKMARKS = """
    CREATE TABLE IF NOT EXISTS result_bookmarks (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        result_id  TEXT NOT NULL,
        name       TEXT NOT NULL,
        reference  TEXT NOT NULL,
        note       TEXT,
        created_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
        UNIQUE (result_id, name)
    )
"""
_DDL_CHECKLIST = """
    CREATE TABLE IF NOT EXISTS result_checklist (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        result_id  TEXT NOT NULL,
        text       TEXT NOT NULL,
        checked    INTEGER NOT NULL DEFAULT 0,
        position   INTEGER NOT NULL DEFAULT 0,
        created_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
    )
"""
_DDL_REACTIONS = """
    CREATE TABLE IF NOT EXISTS result_reactions (
        result_id  TEXT NOT NULL,
        emoji      TEXT NOT NULL,
        count      INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (result_id, emoji)
    )
"""
_DDL_LABELS = """
    CREATE TABLE IF NOT EXISTS labels (
        name        TEXT PRIMARY KEY,
        color       TEXT NOT NULL DEFAULT '#888888',
        description TEXT,
        created_at  TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
    )
"""
_DDL_RESULT_LABELS = """
    CREATE TABLE IF NOT EXISTS result_labels (
        result_id  TEXT NOT NULL,
        label_name TEXT NOT NULL,
        created_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
        PRIMARY KEY (result_id, label_name)
    )
"""
_DDL_COMMENTS = """
    CREATE TABLE IF NOT EXISTS result_comments (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        result_id  TEXT NOT NULL,
        text       TEXT NOT NULL,
        created_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
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
_DDL_ACCESS_LOG = """
    CREATE TABLE IF NOT EXISTS result_access_log (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        result_id  TEXT NOT NULL,
        accessed_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
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
        doc_types  TEXT NOT NULL DEFAULT '[]',
        secret     TEXT,
        active     INTEGER NOT NULL DEFAULT 1,
        created_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
    )
"""
_DDL_WEBHOOK_DELIVERIES = """
    CREATE TABLE IF NOT EXISTS webhook_deliveries (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        webhook_id  TEXT NOT NULL,
        event       TEXT NOT NULL,
        document_id TEXT,
        attempt     INTEGER NOT NULL DEFAULT 1,
        status_code INTEGER,
        success     INTEGER NOT NULL DEFAULT 0,
        error       TEXT,
        delivered_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
    )
"""
_DDL_RESULTS_FTS = """
    CREATE VIRTUAL TABLE IF NOT EXISTS results_fts USING fts5(
        result_id UNINDEXED,
        filename,
        doc_type,
        search_text,
        tokenize='unicode61'
    )
"""
_MIGRATION_WEBHOOKS_COLUMNS = [
    ("doc_types", "TEXT NOT NULL DEFAULT '[]'"),
]
_DDL_API_KEYS = """
    CREATE TABLE IF NOT EXISTS api_keys (
        key_hash   TEXT PRIMARY KEY,
        prefix     TEXT NOT NULL,
        name       TEXT NOT NULL,
        created_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
    )
"""
_DDL_API_KEY_QUOTAS = """
    CREATE TABLE IF NOT EXISTS api_key_quotas (
        prefix      TEXT PRIMARY KEY,
        daily_limit INTEGER NOT NULL,
        updated_at  TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
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
    ("doc_type",         "TEXT"),
    ("pipeline_status",  "TEXT"),
    ("filename",         "TEXT"),
    ("pinned",           "INTEGER NOT NULL DEFAULT 0"),
    ("workflow_status",  "TEXT"),
    ("locked",           "INTEGER NOT NULL DEFAULT 0"),
    ("starred",          "INTEGER NOT NULL DEFAULT 0"),
    ("priority",         "TEXT"),
    ("expires_at",       "TEXT"),
    ("rating",           "INTEGER"),
]

WORKFLOW_STATUSES = {"pending_review", "approved", "rejected", "archived"}
PRIORITY_LEVELS = {"low", "medium", "high", "critical"}
ALLOWED_REACTIONS = {"+1", "-1", "eyes", "check", "red_circle"}
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
        await db.execute(_DDL_ACCESS_LOG)
        await db.execute(_DDL_SCHEDULED)
        await db.execute(_DDL_TEMPLATES)
        await db.execute(_DDL_COLLECTIONS)
        await db.execute(_DDL_COLLECTION_MEMBERS)
        await db.execute(_DDL_WEBHOOK_DELIVERIES)
        await db.execute(_DDL_COMMENTS)
        await db.execute(_DDL_METADATA)
        await db.execute(_DDL_SNAPSHOTS)
        await db.execute(_DDL_RELATIONS)
        await db.execute(_DDL_API_KEY_QUOTAS)
        await db.execute(_DDL_LABELS)
        await db.execute(_DDL_RESULT_LABELS)
        await db.execute(_DDL_REACTIONS)
        await db.execute(_DDL_CHECKLIST)
        await db.execute(_DDL_BOOKMARKS)
        await db.execute(_DDL_RESULT_COSTS)
        await db.execute(_DDL_ATTACHMENTS)
        await db.execute(_DDL_RESULTS_FTS)
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
        for col, col_type in _MIGRATION_WEBHOOKS_COLUMNS:
            try:
                await db.execute(f"ALTER TABLE webhooks ADD COLUMN {col} {col_type}")
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


def _build_search_text(result: PipelineResult) -> str:
    """Extract key text from a PipelineResult for FTS indexing."""
    parts: list[str] = []
    for stage in result.stages:
        data = stage.data or {}
        if stage.module == "generator":
            summary = data.get("summary", "")
            if summary:
                parts.append(summary)
        elif stage.module == "extractor":
            for key, val in data.items():
                if key == "metadata":
                    continue
                if isinstance(val, list):
                    parts.extend(str(v) for v in val if v)
                elif val:
                    parts.append(str(val))
    return " ".join(parts)


async def save_result(result: PipelineResult, document: Document | None = None) -> None:
    doc_type, pipeline_status, filename = _extract_meta(result)
    search_text = _build_search_text(result)
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_RESULTS)
        await db.execute(_DDL_DOCUMENTS)
        await db.execute(_DDL_RESULTS_FTS)
        await db.execute(
            """INSERT OR REPLACE INTO results (id, doc_type, pipeline_status, filename, data)
               VALUES (?, ?, ?, ?, ?)""",
            (str(result.document_id), doc_type, pipeline_status, filename,
             result.model_dump_json()),
        )
        # Keep FTS index in sync — delete old entry first (contentless table)
        await db.execute(
            "DELETE FROM results_fts WHERE result_id = ?", (str(result.document_id),)
        )
        await db.execute(
            "INSERT INTO results_fts (result_id, filename, doc_type, search_text) VALUES (?, ?, ?, ?)",
            (str(result.document_id), filename or "", doc_type or "", search_text),
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
    workflow_status: str | None = None,
    starred: bool | None = None,
    priority: str | None = None,
    include_expired: bool = False,
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
    if workflow_status is not None:
        conditions.append("workflow_status = ?")
        params.append(workflow_status)
    if starred is not None:
        conditions.append("starred = ?")
        params.append(1 if starred else 0)
    if priority is not None:
        conditions.append("priority = ?")
        params.append(priority)
    if not include_expired:
        conditions.append(
            "(expires_at IS NULL OR expires_at > strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))"
        )

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
        await db.execute("DELETE FROM results_fts WHERE result_id = ?", (str(document_id),))
        await db.commit()


async def fts_search(query: str, limit: int = 20, offset: int = 0) -> list[str]:
    """Full-text search across filename, doc_type, and extracted content.

    Returns a list of result_id strings ordered by relevance (BM25 rank).
    """
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_RESULTS_FTS)
        rows = await (
            await db.execute(
                """SELECT result_id FROM results_fts
                   WHERE results_fts MATCH ?
                   ORDER BY rank
                   LIMIT ? OFFSET ?""",
                (query, limit, offset),
            )
        ).fetchall()
    return [r[0] for r in rows]


# ---------------------------------------------------------------------------
# Processing cost tracking
# ---------------------------------------------------------------------------

# Haiku 4.5 pricing (USD per token)
_INPUT_COST_PER_TOKEN = 1.0 / 1_000_000   # $1.00 / 1M
_OUTPUT_COST_PER_TOKEN = 5.0 / 1_000_000  # $5.00 / 1M


def _estimate_cost(input_tokens: int, output_tokens: int) -> float:
    return round(
        input_tokens * _INPUT_COST_PER_TOKEN + output_tokens * _OUTPUT_COST_PER_TOKEN,
        8,
    )


async def save_result_cost(
    result_id: UUID,
    input_tokens: int,
    output_tokens: int,
    model: str,
) -> dict:
    """Upsert the LLM cost record for a result."""
    _ensure_dir()
    cost = _estimate_cost(input_tokens, output_tokens)
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_RESULT_COSTS)
        await db.execute(
            """INSERT INTO result_costs (result_id, input_tokens, output_tokens, model, estimated_cost_usd)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(result_id) DO UPDATE SET
                   input_tokens = excluded.input_tokens,
                   output_tokens = excluded.output_tokens,
                   model = excluded.model,
                   estimated_cost_usd = excluded.estimated_cost_usd""",
            (str(result_id), input_tokens, output_tokens, model, cost),
        )
        await db.commit()
    return {
        "result_id": str(result_id),
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "model": model,
        "estimated_cost_usd": cost,
    }


async def get_result_cost(result_id: UUID) -> dict | None:
    """Return cost record for a result, or None if not tracked."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_RESULT_COSTS)
        row = await (
            await db.execute(
                "SELECT result_id, input_tokens, output_tokens, model, estimated_cost_usd, created_at FROM result_costs WHERE result_id = ?",
                (str(result_id),),
            )
        ).fetchone()
    if row is None:
        return None
    return {
        "result_id": row[0],
        "input_tokens": row[1],
        "output_tokens": row[2],
        "model": row[3],
        "estimated_cost_usd": row[4],
        "created_at": row[5],
    }


async def get_aggregate_cost(limit: int = 100, offset: int = 0) -> dict:
    """Return aggregate token usage and cost across all tracked results."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_RESULT_COSTS)
        row = await (
            await db.execute(
                "SELECT COUNT(*), SUM(input_tokens), SUM(output_tokens), SUM(estimated_cost_usd) FROM result_costs"
            )
        ).fetchone()
    count = row[0] or 0
    return {
        "tracked_results": count,
        "total_input_tokens": row[1] or 0,
        "total_output_tokens": row[2] or 0,
        "total_estimated_cost_usd": round(row[3] or 0.0, 8),
    }


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
            """INSERT OR REPLACE INTO webhooks (id, url, events, doc_types, secret, active)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (str(wh.id), wh.url, json.dumps(wh.events), json.dumps(wh.doc_types),
             wh.secret, int(wh.active)),
        )
        await db.commit()


async def list_webhooks(active_only: bool = False) -> list[WebhookConfig]:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_WEBHOOKS)
        await db.commit()
        where = "WHERE active = 1" if active_only else ""
        async with db.execute(
            f"SELECT id, url, events, doc_types, secret, active FROM webhooks {where}"
        ) as cursor:
            rows = await cursor.fetchall()
    return [
        WebhookConfig(
            id=row[0], url=row[1], events=json.loads(row[2]),
            doc_types=json.loads(row[3] or "[]"),
            secret=row[4], active=bool(row[5]),
        )
        for row in rows
    ]


async def get_webhook(webhook_id: UUID) -> WebhookConfig | None:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_WEBHOOKS)
        await db.commit()
        async with db.execute(
            "SELECT id, url, events, doc_types, secret, active FROM webhooks WHERE id = ?",
            (str(webhook_id),),
        ) as cursor:
            row = await cursor.fetchone()
    if not row:
        return None
    return WebhookConfig(
        id=row[0], url=row[1], events=json.loads(row[2]),
        doc_types=json.loads(row[3] or "[]"),
        secret=row[4], active=bool(row[5]),
    )


async def patch_webhook(
    webhook_id: UUID,
    url: str | None = None,
    events: list[str] | None = None,
    doc_types: list[str] | None = None,
    secret: str | None = None,
) -> WebhookConfig | None:
    """Selectively update webhook fields. Returns updated config, or None if not found."""
    wh = await get_webhook(webhook_id)
    if wh is None:
        return None
    if url is not None:
        wh.url = url
    if events is not None:
        wh.events = events
    if doc_types is not None:
        wh.doc_types = doc_types
    if secret is not None:
        wh.secret = secret
    await save_webhook(wh)
    return wh


async def delete_webhook(webhook_id: UUID) -> bool:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_WEBHOOKS)
        cursor = await db.execute(
            "DELETE FROM webhooks WHERE id = ?", (str(webhook_id),)
        )
        await db.commit()
    return (cursor.rowcount or 0) > 0


async def set_webhook_active(webhook_id: UUID, active: bool) -> bool:
    """Set active=True (resume) or active=False (pause). Returns False if not found."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_WEBHOOKS)
        cursor = await db.execute(
            "UPDATE webhooks SET active = ? WHERE id = ?",
            (int(active), str(webhook_id)),
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


def result_to_markdown(result: PipelineResult) -> str:
    """Render a PipelineResult as a Markdown report."""
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
    filename = meta.get("filename", "—")
    doc_type = classifier_data.get("type", "—")
    confidence = classifier_data.get("confidence", "—")
    method = classifier_data.get("method", "—")
    violations = validator_data.get("violations", [])
    warnings = validator_data.get("warnings", [])

    lines = [
        f"# Document Report",
        "",
        f"| Field | Value |",
        f"|---|---|",
        f"| Document ID | `{result.document_id}` |",
        f"| Filename | {filename} |",
        f"| Status | {result.status} |",
        f"| Duration | {result.total_duration_ms:.1f} ms |",
        "",
        f"## Classification",
        "",
        f"| Field | Value |",
        f"|---|---|",
        f"| Type | {doc_type} |",
        f"| Confidence | {confidence} |",
        f"| Method | {method} |",
    ]

    if fields:
        lines += ["", "## Extracted Fields", "", "| Field | Value |", "|---|---|"]
        for name, values in fields.items():
            display = "; ".join(str(v) for v in values) if isinstance(values, list) else str(values)
            lines.append(f"| {name} | {display} |")

    if violations:
        lines += ["", "## Violations", ""]
        for v in violations:
            lines.append(f"- {v}")

    if warnings:
        lines += ["", "## Warnings", ""]
        for w in warnings:
            lines.append(f"- {w}")

    lines += ["", "## Stages", ""]
    for stage in result.stages:
        lines.append(f"### {stage.module} ({stage.status}, {stage.duration_ms:.1f} ms)")
        if stage.errors:
            for e in stage.errors:
                lines.append(f"- Error: {e}")

    return "\n".join(lines) + "\n"


def _xml_escape(value: str) -> str:
    return (
        value.replace("&", "&amp;")
             .replace("<", "&lt;")
             .replace(">", "&gt;")
             .replace('"', "&quot;")
             .replace("'", "&apos;")
    )


def result_to_xml(result: PipelineResult) -> str:
    """Render a PipelineResult as an XML document."""
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
    violations = validator_data.get("violations", [])
    warnings = validator_data.get("warnings", [])

    def tag(name: str, value: object, indent: int = 2) -> str:
        pad = " " * indent
        s = _xml_escape(str(value)) if value is not None else ""
        return f"{pad}<{name}>{s}</{name}>"

    lines = ['<?xml version="1.0" encoding="UTF-8"?>', "<result>"]
    lines.append(tag("document_id", result.document_id))
    lines.append(tag("filename", meta.get("filename", "")))
    lines.append(tag("status", result.status))
    lines.append(tag("total_duration_ms", result.total_duration_ms))

    lines.append("  <classification>")
    lines.append(tag("type", classifier_data.get("type", ""), 4))
    lines.append(tag("confidence", classifier_data.get("confidence", ""), 4))
    lines.append(tag("method", classifier_data.get("method", ""), 4))
    lines.append("  </classification>")

    if fields:
        lines.append("  <fields>")
        for name, values in fields.items():
            display = "; ".join(str(v) for v in values) if isinstance(values, list) else str(values)
            lines.append(f"    <{name}>{_xml_escape(display)}</{name}>")
        lines.append("  </fields>")

    if violations:
        lines.append("  <violations>")
        for v in violations:
            lines.append(tag("violation", v, 4))
        lines.append("  </violations>")

    if warnings:
        lines.append("  <warnings>")
        for w in warnings:
            lines.append(tag("warning", w, 4))
        lines.append("  </warnings>")

    lines.append("  <stages>")
    for stage in result.stages:
        lines.append("    <stage>")
        lines.append(tag("module", stage.module, 6))
        lines.append(tag("status", stage.status, 6))
        lines.append(tag("duration_ms", stage.duration_ms, 6))
        lines.append("    </stage>")
    lines.append("  </stages>")

    lines.append("</result>")
    return "\n".join(lines) + "\n"


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


async def set_api_key_quota(prefix: str, daily_limit: int) -> None:
    """Upsert a daily document-processing limit for an API key prefix."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_API_KEY_QUOTAS)
        await db.execute(
            """INSERT INTO api_key_quotas (prefix, daily_limit, updated_at)
               VALUES (?, ?, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
               ON CONFLICT(prefix) DO UPDATE SET
                 daily_limit = excluded.daily_limit,
                 updated_at  = excluded.updated_at""",
            (prefix, daily_limit),
        )
        await db.commit()


async def get_api_key_quota(prefix: str) -> dict | None:
    """Return quota info for a prefix, or None if the prefix doesn't exist as an API key."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_API_KEYS)
        await db.execute(_DDL_API_KEY_QUOTAS)
        await db.execute(_DDL_RESULTS)
        await db.commit()
        # Verify the prefix actually exists
        async with db.execute(
            "SELECT prefix, name FROM api_keys WHERE prefix = ?", (prefix,)
        ) as cur:
            key_row = await cur.fetchone()
        if not key_row:
            return None
        # Get limit if set
        async with db.execute(
            "SELECT daily_limit FROM api_key_quotas WHERE prefix = ?", (prefix,)
        ) as cur:
            quota_row = await cur.fetchone()
        daily_limit: int | None = quota_row[0] if quota_row else None
        # Count results created today
        async with db.execute(
            "SELECT COUNT(*) FROM results WHERE created_at >= strftime('%Y-%m-%dT00:00:00Z', 'now')"
        ) as cur:
            used_today: int = (await cur.fetchone())[0]  # type: ignore[index]
    remaining: int | None = max(0, daily_limit - used_today) if daily_limit is not None else None
    return {
        "prefix": prefix,
        "daily_limit": daily_limit,
        "used_today": used_today,
        "remaining": remaining,
    }


async def delete_api_key_quota(prefix: str) -> bool:
    """Remove the daily limit for an API key prefix. Returns False if no limit was set."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_API_KEY_QUOTAS)
        cursor = await db.execute(
            "DELETE FROM api_key_quotas WHERE prefix = ?", (prefix,)
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


async def get_tag_stats() -> list[dict]:
    """Return all tags with their result-count, sorted by count descending."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_TAGS)
        await db.commit()
        async with db.execute(
            "SELECT tag, COUNT(*) as cnt FROM tags GROUP BY tag ORDER BY cnt DESC, tag ASC"
        ) as cur:
            rows = await cur.fetchall()
    return [{"tag": r[0], "count": r[1]} for r in rows]


async def get_all_tags(prefix: str | None = None, limit: int = 50) -> list[str]:
    """Return unique tag names, optionally filtered by prefix, ordered alphabetically."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_TAGS)
        await db.commit()
        if prefix:
            pattern = prefix.strip().lower() + "%"
            async with db.execute(
                "SELECT DISTINCT tag FROM tags WHERE tag LIKE ? ORDER BY tag LIMIT ?",
                (pattern, limit),
            ) as cur:
                rows = await cur.fetchall()
        else:
            async with db.execute(
                "SELECT DISTINCT tag FROM tags ORDER BY tag LIMIT ?",
                (limit,),
            ) as cur:
                rows = await cur.fetchall()
    return [r[0] for r in rows]


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


async def set_starred(result_id: UUID, starred: bool) -> bool:
    """Star or unstar a result. Returns False if the result does not exist."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_RESULTS)
        cursor = await db.execute(
            "UPDATE results SET starred = ? WHERE id = ?",
            (1 if starred else 0, str(result_id)),
        )
        await db.commit()
    return (cursor.rowcount or 0) > 0


async def is_starred(result_id: UUID) -> bool | None:
    """Return starred state, or None if result does not exist."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_RESULTS)
        await db.commit()
        async with db.execute(
            "SELECT starred FROM results WHERE id = ?", (str(result_id),)
        ) as cur:
            row = await cur.fetchone()
    return bool(row[0]) if row is not None else None


# ---------------------------------------------------------------------------
# Result priority
# ---------------------------------------------------------------------------

async def set_result_priority(result_id: UUID, priority: str | None) -> bool:
    """Set or clear priority. Returns False if result not found."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_RESULTS)
        cursor = await db.execute(
            "UPDATE results SET priority = ? WHERE id = ?",
            (priority, str(result_id)),
        )
        await db.commit()
    return (cursor.rowcount or 0) > 0


async def get_result_priority(result_id: UUID) -> str | None:
    """Return priority, or None if unset or result not found."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_RESULTS)
        await db.commit()
        async with db.execute(
            "SELECT priority FROM results WHERE id = ?", (str(result_id),)
        ) as cur:
            row = await cur.fetchone()
    if row is None:
        return None
    return row[0]


# ---------------------------------------------------------------------------
# Result rating (1–5)
# ---------------------------------------------------------------------------

async def set_result_rating(result_id: UUID, rating: int) -> bool:
    """Set a 1–5 rating. Returns False if result not found."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_RESULTS)
        cursor = await db.execute(
            "UPDATE results SET rating = ? WHERE id = ?", (rating, str(result_id))
        )
        await db.commit()
    return (cursor.rowcount or 0) > 0


async def get_result_rating(result_id: UUID) -> int | None:
    """Return rating, or None if unset or result not found."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_RESULTS)
        await db.commit()
        row = await (
            await db.execute("SELECT rating FROM results WHERE id = ?", (str(result_id),))
        ).fetchone()
    if row is None:
        return None
    return row[0]


async def delete_result_rating(result_id: UUID) -> bool:
    """Clear the rating. Returns False if result not found."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_RESULTS)
        cursor = await db.execute(
            "UPDATE results SET rating = NULL WHERE id = ?", (str(result_id),)
        )
        await db.commit()
    return (cursor.rowcount or 0) > 0


# ---------------------------------------------------------------------------
# Result expiry (TTL)
# ---------------------------------------------------------------------------

async def set_result_expiry(result_id: UUID, expires_at: str | None) -> bool:
    """Set or clear the expiry timestamp (ISO 8601). Returns False if result not found."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_RESULTS)
        cursor = await db.execute(
            "UPDATE results SET expires_at = ? WHERE id = ?",
            (expires_at, str(result_id)),
        )
        await db.commit()
    return (cursor.rowcount or 0) > 0


async def get_result_expiry(result_id: UUID) -> str | None:
    """Return expires_at for a result, or None if unset or result not found."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_RESULTS)
        await db.commit()
        async with db.execute(
            "SELECT expires_at FROM results WHERE id = ?", (str(result_id),)
        ) as cur:
            row = await cur.fetchone()
    if row is None:
        return None
    return row[0]


async def purge_expired_results() -> int:
    """Delete all results whose expires_at is in the past. Returns count deleted."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_RESULTS)
        cursor = await db.execute(
            "DELETE FROM results WHERE expires_at IS NOT NULL "
            "AND expires_at <= strftime('%Y-%m-%dT%H:%M:%SZ', 'now')"
        )
        await db.commit()
    return cursor.rowcount or 0


async def set_result_locked(result_id: UUID, locked: bool) -> bool:
    """Lock or unlock a result. Returns False if the result does not exist."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_RESULTS)
        cursor = await db.execute(
            "UPDATE results SET locked = ? WHERE id = ?",
            (1 if locked else 0, str(result_id)),
        )
        await db.commit()
    return (cursor.rowcount or 0) > 0


async def is_result_locked(result_id: UUID) -> bool | None:
    """Return lock state, or None if result does not exist."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_RESULTS)
        await db.commit()
        async with db.execute(
            "SELECT locked FROM results WHERE id = ?", (str(result_id),)
        ) as cur:
            row = await cur.fetchone()
    return bool(row[0]) if row is not None else None


# ---------------------------------------------------------------------------
# Similarity search
# ---------------------------------------------------------------------------

def _field_tokens(result: PipelineResult) -> set[str]:
    """Extract a flat set of normalised tokens from a result's extracted fields."""
    tokens: set[str] = set()
    for stage in result.stages:
        if stage.module in ("refiner", "extractor"):
            for values in (stage.data.get("fields") or {}).values():
                if isinstance(values, list):
                    for v in values:
                        tokens.update(str(v).lower().split())
                elif values:
                    tokens.update(str(values).lower().split())
    return tokens


def _jaccard(a: set, b: set) -> float:
    if not a and not b:
        return 1.0
    union = a | b
    return len(a & b) / len(union) if union else 0.0


async def find_similar(
    reference_id: UUID,
    top_n: int = 5,
    min_score: float = 0.0,
) -> list[dict]:
    """Return up to top_n results most similar to reference_id by field-token Jaccard."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_RESULTS)
        await db.commit()
        async with db.execute(
            "SELECT id, data FROM results WHERE id != ?", (str(reference_id),)
        ) as cur:
            rows = await cur.fetchall()
        async with db.execute(
            "SELECT data FROM results WHERE id = ?", (str(reference_id),)
        ) as cur:
            ref_row = await cur.fetchone()

    if not ref_row:
        return []

    ref = PipelineResult.model_validate_json(ref_row[0])
    ref_tokens = _field_tokens(ref)

    scored: list[tuple[float, str, str]] = []
    for row_id, row_data in rows:
        candidate = PipelineResult.model_validate_json(row_data)
        score = _jaccard(ref_tokens, _field_tokens(candidate))
        if score >= min_score:
            scored.append((score, row_id, row_data))

    scored.sort(key=lambda x: x[0], reverse=True)

    return [
        {
            "document_id": item[1],
            "score": round(item[0], 4),
            "result": PipelineResult.model_validate_json(item[2]).model_dump(mode="json"),
        }
        for item in scored[:top_n]
    ]


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


async def get_collection_stats(collection_id: str) -> dict | None:
    """Return aggregate stats for results in a collection, or None if collection doesn't exist."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_RESULTS)
        await db.execute(_DDL_COLLECTIONS)
        await db.execute(_DDL_COLLECTION_MEMBERS)
        await db.commit()
        # Verify collection exists
        async with db.execute(
            "SELECT id FROM collections WHERE id = ?", (collection_id,)
        ) as cur:
            if not await cur.fetchone():
                return None
        # Totals
        async with db.execute(
            """SELECT
                COUNT(*) AS total,
                SUM(r.pinned) AS pinned,
                SUM(r.locked) AS locked,
                AVG(CAST(json_extract(r.data, '$.total_duration_ms') AS REAL)) AS avg_ms
               FROM results r
               JOIN collection_members cm ON cm.result_id = r.id
               WHERE cm.collection_id = ?""",
            (collection_id,),
        ) as cur:
            row = await cur.fetchone()
        total = row[0] or 0
        pinned = int(row[1] or 0)
        locked = int(row[2] or 0)
        avg_ms = round(float(row[3]), 2) if row[3] is not None else None
        # By doc_type
        async with db.execute(
            """SELECT r.doc_type, COUNT(*) AS cnt
               FROM results r
               JOIN collection_members cm ON cm.result_id = r.id
               WHERE cm.collection_id = ?
               GROUP BY r.doc_type ORDER BY cnt DESC""",
            (collection_id,),
        ) as cur:
            by_doc_type = [{"doc_type": r[0], "count": r[1]} for r in await cur.fetchall()]
        # By status
        async with db.execute(
            """SELECT r.pipeline_status, COUNT(*) AS cnt
               FROM results r
               JOIN collection_members cm ON cm.result_id = r.id
               WHERE cm.collection_id = ?
               GROUP BY r.pipeline_status ORDER BY cnt DESC""",
            (collection_id,),
        ) as cur:
            by_status = [{"status": r[0], "count": r[1]} for r in await cur.fetchall()]
    return {
        "collection_id": collection_id,
        "total": total,
        "pinned": pinned,
        "locked": locked,
        "avg_duration_ms": avg_ms,
        "by_doc_type": by_doc_type,
        "by_status": by_status,
    }


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


# ---------------------------------------------------------------------------
# Result access log
# ---------------------------------------------------------------------------

async def record_result_access(result_id: UUID) -> None:
    """Record that a result was accessed (fetched via GET)."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_ACCESS_LOG)
        await db.execute(
            "INSERT INTO result_access_log (result_id) VALUES (?)",
            (str(result_id),),
        )
        await db.commit()


async def get_result_access_log(
    result_id: UUID, limit: int = 100, offset: int = 0
) -> tuple[list[dict], int]:
    """Return paginated access log entries for a result."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_ACCESS_LOG)
        await db.commit()
        async with db.execute(
            "SELECT COUNT(*) FROM result_access_log WHERE result_id = ?",
            (str(result_id),),
        ) as cur:
            total: int = (await cur.fetchone())[0]  # type: ignore[index]
        async with db.execute(
            "SELECT id, accessed_at FROM result_access_log "
            "WHERE result_id = ? ORDER BY id DESC LIMIT ? OFFSET ?",
            (str(result_id), limit, offset),
        ) as cur:
            rows = await cur.fetchall()
    return [{"id": r[0], "accessed_at": r[1]} for r in rows], total


async def get_result_access_count(result_id: UUID) -> int:
    """Return total number of times a result has been accessed."""
    _, total = await get_result_access_log(result_id, limit=0)
    return total


# ---------------------------------------------------------------------------
# Webhook delivery log
# ---------------------------------------------------------------------------

async def log_webhook_delivery(
    webhook_id: str,
    event: str,
    document_id: str | None,
    attempt: int,
    status_code: int | None,
    success: bool,
    error: str | None = None,
) -> None:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_WEBHOOK_DELIVERIES)
        await db.execute(
            """INSERT INTO webhook_deliveries
               (webhook_id, event, document_id, attempt, status_code, success, error)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (webhook_id, event, document_id, attempt, status_code, int(success), error),
        )
        await db.commit()


async def get_webhook_deliveries(
    webhook_id: str, limit: int = 50, offset: int = 0
) -> tuple[list[dict], int]:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_WEBHOOK_DELIVERIES)
        await db.commit()
        async with db.execute(
            "SELECT COUNT(*) FROM webhook_deliveries WHERE webhook_id = ?", (webhook_id,)
        ) as cur:
            total: int = (await cur.fetchone())[0]  # type: ignore[index]
        async with db.execute(
            """SELECT id, webhook_id, event, document_id, attempt, status_code, success, error, delivered_at
               FROM webhook_deliveries WHERE webhook_id = ?
               ORDER BY id DESC LIMIT ? OFFSET ?""",
            (webhook_id, limit, offset),
        ) as cur:
            rows = await cur.fetchall()
    return [
        {
            "id": r[0],
            "webhook_id": r[1],
            "event": r[2],
            "document_id": r[3],
            "attempt": r[4],
            "status_code": r[5],
            "success": bool(r[6]),
            "error": r[7],
            "delivered_at": r[8],
        }
        for r in rows
    ], total


async def get_webhook_delivery(delivery_id: int) -> dict | None:
    """Fetch a single webhook delivery record by its integer ID."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_WEBHOOK_DELIVERIES)
        await db.commit()
        async with db.execute(
            """SELECT id, webhook_id, event, document_id, attempt, status_code, success, error, delivered_at
               FROM webhook_deliveries WHERE id = ?""",
            (delivery_id,),
        ) as cur:
            row = await cur.fetchone()
    if not row:
        return None
    return {
        "id": row[0],
        "webhook_id": row[1],
        "event": row[2],
        "document_id": row[3],
        "attempt": row[4],
        "status_code": row[5],
        "success": bool(row[6]),
        "error": row[7],
        "delivered_at": row[8],
    }


async def get_webhook_delivery_stats(webhook_id: str) -> dict:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_WEBHOOK_DELIVERIES)
        await db.commit()
        async with db.execute(
            """SELECT
                COUNT(*) AS total,
                SUM(success) AS successes,
                COUNT(*) - SUM(success) AS failures,
                AVG(attempt) AS avg_attempts
               FROM webhook_deliveries WHERE webhook_id = ?""",
            (webhook_id,),
        ) as cur:
            row = await cur.fetchone()
    total = row[0] or 0
    successes = int(row[1] or 0)
    failures = int(row[2] or 0)
    avg_attempts = round(float(row[3] or 0), 2)
    success_rate = round(successes / total, 4) if total else 0.0
    return {
        "total": total,
        "successes": successes,
        "failures": failures,
        "success_rate": success_rate,
        "avg_attempts": avg_attempts,
    }


# ---------------------------------------------------------------------------
# Global webhook stats
# ---------------------------------------------------------------------------

async def get_global_webhook_stats() -> dict:
    """Return aggregate delivery stats across all webhooks."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_WEBHOOK_DELIVERIES)
        await db.commit()
        async with db.execute(
            """SELECT
                COUNT(*) AS total,
                SUM(success) AS successes,
                COUNT(*) - SUM(success) AS failures,
                AVG(attempt) AS avg_attempts
               FROM webhook_deliveries"""
        ) as cur:
            row = await cur.fetchone()
        async with db.execute(
            """SELECT event, COUNT(*) AS cnt, SUM(success) AS ok
               FROM webhook_deliveries GROUP BY event ORDER BY cnt DESC"""
        ) as cur:
            event_rows = await cur.fetchall()
        async with db.execute(
            """SELECT webhook_id, COUNT(*) AS total, SUM(success) AS ok
               FROM webhook_deliveries GROUP BY webhook_id
               ORDER BY (COUNT(*) - SUM(success)) DESC, total DESC LIMIT 5"""
        ) as cur:
            failing_rows = await cur.fetchall()

    total = row[0] or 0
    successes = int(row[1] or 0)
    failures = int(row[2] or 0)
    avg_attempts = round(float(row[3] or 0), 2)
    success_rate = round(successes / total, 4) if total else 0.0
    by_event = [
        {
            "event": r[0],
            "total": r[1],
            "successes": int(r[2] or 0),
            "failures": r[1] - int(r[2] or 0),
        }
        for r in event_rows
    ]
    top_failing = [
        {
            "webhook_id": r[0],
            "total": r[1],
            "successes": int(r[2] or 0),
            "failures": r[1] - int(r[2] or 0),
        }
        for r in failing_rows
    ]
    return {
        "total": total,
        "successes": successes,
        "failures": failures,
        "success_rate": success_rate,
        "avg_attempts": avg_attempts,
        "by_event": by_event,
        "top_failing_webhooks": top_failing,
    }


# ---------------------------------------------------------------------------
# Labels
# ---------------------------------------------------------------------------

async def create_label(name: str, color: str = "#888888", description: str | None = None) -> dict:
    """Create a label. Returns the created label or raises if name already exists."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_LABELS)
        await db.execute(
            "INSERT INTO labels (name, color, description) VALUES (?, ?, ?)",
            (name, color, description),
        )
        await db.commit()
        async with db.execute(
            "SELECT name, color, description, created_at FROM labels WHERE name = ?",
            (name,),
        ) as cur:
            row = await cur.fetchone()
    return {"name": row[0], "color": row[1], "description": row[2], "created_at": row[3]}


async def list_labels() -> list[dict]:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_LABELS)
        await db.commit()
        async with db.execute(
            "SELECT name, color, description, created_at FROM labels ORDER BY name"
        ) as cur:
            rows = await cur.fetchall()
    return [{"name": r[0], "color": r[1], "description": r[2], "created_at": r[3]} for r in rows]


async def delete_label(name: str) -> bool:
    """Delete a label and all its result assignments. Returns False if label not found."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_LABELS)
        await db.execute(_DDL_RESULT_LABELS)
        cursor = await db.execute("DELETE FROM labels WHERE name = ?", (name,))
        if (cursor.rowcount or 0) == 0:
            return False
        await db.execute("DELETE FROM result_labels WHERE label_name = ?", (name,))
        await db.commit()
    return True


async def get_label(name: str) -> dict | None:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_LABELS)
        await db.commit()
        async with db.execute(
            "SELECT name, color, description, created_at FROM labels WHERE name = ?", (name,)
        ) as cur:
            row = await cur.fetchone()
    if row is None:
        return None
    return {"name": row[0], "color": row[1], "description": row[2], "created_at": row[3]}


async def apply_label(result_id: UUID, label_name: str) -> None:
    """Apply a label to a result (idempotent)."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_RESULT_LABELS)
        await db.execute(
            "INSERT OR IGNORE INTO result_labels (result_id, label_name) VALUES (?, ?)",
            (str(result_id), label_name),
        )
        await db.commit()


async def remove_label_from_result(result_id: UUID, label_name: str) -> bool:
    """Remove a label from a result. Returns False if assignment did not exist."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_RESULT_LABELS)
        cursor = await db.execute(
            "DELETE FROM result_labels WHERE result_id = ? AND label_name = ?",
            (str(result_id), label_name),
        )
        await db.commit()
    return (cursor.rowcount or 0) > 0


async def get_result_labels(result_id: UUID) -> list[dict]:
    """Return all labels applied to a result, with their colors."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_LABELS)
        await db.execute(_DDL_RESULT_LABELS)
        await db.commit()
        async with db.execute(
            """SELECT l.name, l.color, l.description
               FROM result_labels rl JOIN labels l ON rl.label_name = l.name
               WHERE rl.result_id = ? ORDER BY l.name""",
            (str(result_id),),
        ) as cur:
            rows = await cur.fetchall()
    return [{"name": r[0], "color": r[1], "description": r[2]} for r in rows]


async def search_results_by_label(label_name: str, limit: int = 50, offset: int = 0) -> tuple[list, int]:
    """Return results tagged with a given label."""
    from document_processor.models import PipelineResult
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_RESULTS)
        await db.execute(_DDL_RESULT_LABELS)
        await db.commit()
        async with db.execute(
            "SELECT COUNT(*) FROM result_labels WHERE label_name = ?", (label_name,)
        ) as cur:
            total: int = (await cur.fetchone())[0]  # type: ignore[index]
        async with db.execute(
            """SELECT r.data FROM results r
               JOIN result_labels rl ON r.id = rl.result_id
               WHERE rl.label_name = ?
               ORDER BY r.created_at DESC LIMIT ? OFFSET ?""",
            (label_name, limit, offset),
        ) as cur:
            rows = await cur.fetchall()
    return [PipelineResult.model_validate_json(r[0]) for r in rows], total


# ---------------------------------------------------------------------------
# Result comments
# ---------------------------------------------------------------------------

async def add_comment(result_id: UUID, text: str) -> dict:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_COMMENTS)
        cursor = await db.execute(
            "INSERT INTO result_comments (result_id, text) VALUES (?, ?)",
            (str(result_id), text),
        )
        row_id = cursor.lastrowid
        await db.commit()
        async with db.execute(
            "SELECT id, result_id, text, created_at FROM result_comments WHERE id = ?",
            (row_id,),
        ) as cur:
            row = await cur.fetchone()
    return {"id": row[0], "result_id": row[1], "text": row[2], "created_at": row[3]}


async def get_comments(
    result_id: UUID, limit: int = 50, offset: int = 0
) -> tuple[list[dict], int]:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_COMMENTS)
        await db.commit()
        async with db.execute(
            "SELECT COUNT(*) FROM result_comments WHERE result_id = ?",
            (str(result_id),),
        ) as cur:
            total: int = (await cur.fetchone())[0]  # type: ignore[index]
        async with db.execute(
            """SELECT id, result_id, text, created_at FROM result_comments
               WHERE result_id = ? ORDER BY id ASC LIMIT ? OFFSET ?""",
            (str(result_id), limit, offset),
        ) as cur:
            rows = await cur.fetchall()
    return [
        {"id": r[0], "result_id": r[1], "text": r[2], "created_at": r[3]}
        for r in rows
    ], total


async def delete_comment(result_id: UUID, comment_id: int) -> bool:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_COMMENTS)
        cursor = await db.execute(
            "DELETE FROM result_comments WHERE id = ? AND result_id = ?",
            (comment_id, str(result_id)),
        )
        await db.commit()
    return (cursor.rowcount or 0) > 0


# ---------------------------------------------------------------------------
# Batch export
# ---------------------------------------------------------------------------

_EXPORT_EXTENSIONS = {"json": "json", "csv": "csv", "markdown": "md", "xml": "xml"}


async def batch_export_zip(ids: list[UUID], format: str = "json") -> bytes:
    """Export multiple results as a ZIP archive. Missing IDs are silently skipped."""
    ext = _EXPORT_EXTENSIONS.get(format, "json")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for result_id in ids:
            result = await get_result(result_id)
            if result is None:
                continue
            if format == "csv":
                content = result_to_csv(result).encode()
            elif format == "markdown":
                content = result_to_markdown(result).encode()
            elif format == "xml":
                content = result_to_xml(result).encode()
            else:
                content = result.model_dump_json(indent=2).encode()
            zf.writestr(f"{result_id}.{ext}", content)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Workflow status
# ---------------------------------------------------------------------------

async def set_workflow_status(result_id: UUID, status: str) -> bool:
    """Set the workflow_status on a result. Returns False if result not found."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_RESULTS)
        await db.commit()
        cursor = await db.execute(
            "UPDATE results SET workflow_status = ? WHERE id = ?",
            (status, str(result_id)),
        )
        await db.commit()
    return (cursor.rowcount or 0) > 0


async def get_workflow_status(result_id: UUID) -> str | None:
    """Return the workflow_status for a result, or None if not set / not found."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_RESULTS)
        await db.commit()
        async with db.execute(
            "SELECT workflow_status FROM results WHERE id = ?", (str(result_id),)
        ) as cur:
            row = await cur.fetchone()
    if row is None:
        return None
    return row[0]  # may be None if column is NULL


# ---------------------------------------------------------------------------
# Custom key-value metadata
# ---------------------------------------------------------------------------

async def set_metadata(result_id: UUID, key: str, value: object) -> None:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_METADATA)
        await db.execute(
            """INSERT INTO result_metadata (result_id, key, value, updated_at)
               VALUES (?, ?, ?, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
               ON CONFLICT(result_id, key) DO UPDATE SET
                 value = excluded.value,
                 updated_at = excluded.updated_at""",
            (str(result_id), key, json.dumps(value)),
        )
        await db.commit()


async def get_metadata(result_id: UUID) -> dict:
    """Return all key-value metadata for a result as a dict."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_METADATA)
        await db.commit()
        async with db.execute(
            "SELECT key, value, updated_at FROM result_metadata WHERE result_id = ? ORDER BY key",
            (str(result_id),),
        ) as cur:
            rows = await cur.fetchall()
    return {
        r[0]: {"value": json.loads(r[1]), "updated_at": r[2]}
        for r in rows
    }


async def get_metadata_key(result_id: UUID, key: str) -> dict | None:
    """Return a single metadata entry or None if not found."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_METADATA)
        await db.commit()
        async with db.execute(
            "SELECT value, updated_at FROM result_metadata WHERE result_id = ? AND key = ?",
            (str(result_id), key),
        ) as cur:
            row = await cur.fetchone()
    if row is None:
        return None
    return {"key": key, "value": json.loads(row[0]), "updated_at": row[1]}


async def delete_metadata_key(result_id: UUID, key: str) -> bool:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_METADATA)
        cursor = await db.execute(
            "DELETE FROM result_metadata WHERE result_id = ? AND key = ?",
            (str(result_id), key),
        )
        await db.commit()
    return (cursor.rowcount or 0) > 0


# ---------------------------------------------------------------------------
# Enhanced statistics
# ---------------------------------------------------------------------------

async def get_stats_timeline(days: int = 30) -> list[dict]:
    """Return document count per day for the last N days (newest first)."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_RESULTS)
        await db.commit()
        async with db.execute(
            """SELECT substr(created_at, 1, 10) AS day, COUNT(*) AS count
               FROM results
               WHERE created_at >= strftime('%Y-%m-%dT%H:%M:%SZ', datetime('now', ?))
               GROUP BY day
               ORDER BY day DESC""",
            (f"-{days} days",),
        ) as cur:
            rows = await cur.fetchall()
    return [{"date": r[0], "count": r[1]} for r in rows]


async def get_stats_stage_timing() -> list[dict]:
    """Return per-module timing stats (avg, min, max duration_ms) computed from stored JSON."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_RESULTS)
        await db.commit()
        # Extract stage data via JSON functions — stages is a JSON array
        async with db.execute("SELECT data FROM results") as cur:
            rows = await cur.fetchall()

    module_times: dict[str, list[float]] = {}
    for (raw,) in rows:
        try:
            result = json.loads(raw)
            for stage in result.get("stages", []):
                mod = stage.get("module")
                dur = stage.get("duration_ms")
                if mod and isinstance(dur, (int, float)):
                    module_times.setdefault(mod, []).append(float(dur))
        except Exception:
            pass

    out = []
    for module, times in sorted(module_times.items()):
        times_sorted = sorted(times)
        n = len(times_sorted)
        out.append({
            "module": module,
            "count": n,
            "avg_ms": round(sum(times_sorted) / n, 2),
            "min_ms": round(times_sorted[0], 2),
            "max_ms": round(times_sorted[-1], 2),
            "p50_ms": round(times_sorted[n // 2], 2),
            "p95_ms": round(times_sorted[min(int(n * 0.95), n - 1)], 2),
        })
    return out


async def get_stats_error_rates() -> list[dict]:
    """Return per-doc_type success/failure counts and error rate."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_RESULTS)
        await db.commit()
        async with db.execute(
            """SELECT
                doc_type,
                COUNT(*) AS total,
                SUM(CASE WHEN pipeline_status = 'success' THEN 1 ELSE 0 END) AS successes,
                SUM(CASE WHEN pipeline_status != 'success' THEN 1 ELSE 0 END) AS failures
               FROM results
               GROUP BY doc_type
               ORDER BY total DESC"""
        ) as cur:
            rows = await cur.fetchall()
    return [
        {
            "doc_type": r[0] or "unknown",
            "total": r[1],
            "successes": r[2],
            "failures": r[3],
            "error_rate": round(r[3] / r[1], 4) if r[1] else 0.0,
        }
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Result bookmarks
# ---------------------------------------------------------------------------

async def upsert_bookmark(result_id: UUID, name: str, reference: str, note: str | None = None) -> dict:
    """Create or update a named bookmark. Name is unique per result."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_BOOKMARKS)
        await db.execute(
            """INSERT INTO result_bookmarks (result_id, name, reference, note)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(result_id, name) DO UPDATE SET
                 reference = excluded.reference,
                 note = excluded.note""",
            (str(result_id), name, reference, note),
        )
        await db.commit()
        async with db.execute(
            "SELECT id, name, reference, note, created_at FROM result_bookmarks "
            "WHERE result_id = ? AND name = ?",
            (str(result_id), name),
        ) as cur:
            row = await cur.fetchone()
    return {"id": row[0], "name": row[1], "reference": row[2], "note": row[3], "created_at": row[4]}


async def get_bookmarks(result_id: UUID) -> list[dict]:
    """Return all bookmarks for a result, ordered by name."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_BOOKMARKS)
        await db.commit()
        async with db.execute(
            "SELECT id, name, reference, note, created_at FROM result_bookmarks "
            "WHERE result_id = ? ORDER BY name ASC",
            (str(result_id),),
        ) as cur:
            rows = await cur.fetchall()
    return [{"id": r[0], "name": r[1], "reference": r[2], "note": r[3], "created_at": r[4]} for r in rows]


async def get_bookmark(result_id: UUID, name: str) -> dict | None:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_BOOKMARKS)
        await db.commit()
        async with db.execute(
            "SELECT id, name, reference, note, created_at FROM result_bookmarks "
            "WHERE result_id = ? AND name = ?",
            (str(result_id), name),
        ) as cur:
            row = await cur.fetchone()
    if row is None:
        return None
    return {"id": row[0], "name": row[1], "reference": row[2], "note": row[3], "created_at": row[4]}


async def delete_bookmark(result_id: UUID, name: str) -> bool:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_BOOKMARKS)
        cursor = await db.execute(
            "DELETE FROM result_bookmarks WHERE result_id = ? AND name = ?",
            (str(result_id), name),
        )
        await db.commit()
    return (cursor.rowcount or 0) > 0


# ---------------------------------------------------------------------------
# Result attachments
# ---------------------------------------------------------------------------

async def save_attachment(
    result_id: UUID,
    filename: str,
    mimetype: str,
    content: bytes,
) -> dict:
    _ensure_dir()
    size = len(content)
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_ATTACHMENTS)
        cursor = await db.execute(
            """INSERT INTO result_attachments (result_id, filename, mimetype, size, content)
               VALUES (?, ?, ?, ?, ?)""",
            (str(result_id), filename, mimetype, size, content),
        )
        row_id = cursor.lastrowid
        await db.commit()
        row = await (
            await db.execute(
                "SELECT id, filename, mimetype, size, created_at FROM result_attachments WHERE id = ?",
                (row_id,),
            )
        ).fetchone()
    return {"id": row[0], "filename": row[1], "mimetype": row[2], "size": row[3], "created_at": row[4]}


async def list_attachments(result_id: UUID) -> list[dict]:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_ATTACHMENTS)
        rows = await (
            await db.execute(
                "SELECT id, filename, mimetype, size, created_at FROM result_attachments WHERE result_id = ? ORDER BY created_at",
                (str(result_id),),
            )
        ).fetchall()
    return [{"id": r[0], "filename": r[1], "mimetype": r[2], "size": r[3], "created_at": r[4]} for r in rows]


async def get_attachment(result_id: UUID, attachment_id: int) -> dict | None:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_ATTACHMENTS)
        row = await (
            await db.execute(
                "SELECT id, filename, mimetype, size, content, created_at FROM result_attachments WHERE id = ? AND result_id = ?",
                (attachment_id, str(result_id)),
            )
        ).fetchone()
    if row is None:
        return None
    return {"id": row[0], "filename": row[1], "mimetype": row[2], "size": row[3], "content": row[4], "created_at": row[5]}


async def delete_attachment(result_id: UUID, attachment_id: int) -> bool:
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_ATTACHMENTS)
        cursor = await db.execute(
            "DELETE FROM result_attachments WHERE id = ? AND result_id = ?",
            (attachment_id, str(result_id)),
        )
        await db.commit()
    return (cursor.rowcount or 0) > 0


# ---------------------------------------------------------------------------
# Result checklist
# ---------------------------------------------------------------------------

async def add_checklist_item(result_id: UUID, text: str) -> dict:
    """Append a new checklist item. Position = current max + 1."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_CHECKLIST)
        async with db.execute(
            "SELECT COALESCE(MAX(position), -1) FROM result_checklist WHERE result_id = ?",
            (str(result_id),),
        ) as cur:
            pos: int = (await cur.fetchone())[0] + 1  # type: ignore[index]
        cursor = await db.execute(
            "INSERT INTO result_checklist (result_id, text, position) VALUES (?, ?, ?)",
            (str(result_id), text, pos),
        )
        item_id = cursor.lastrowid
        await db.commit()
        async with db.execute(
            "SELECT id, text, checked, position, created_at FROM result_checklist WHERE id = ?",
            (item_id,),
        ) as cur:
            row = await cur.fetchone()
    return {"id": row[0], "text": row[1], "checked": bool(row[2]), "position": row[3], "created_at": row[4]}


async def get_checklist(result_id: UUID) -> list[dict]:
    """Return all checklist items for a result, ordered by position."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_CHECKLIST)
        await db.commit()
        async with db.execute(
            "SELECT id, text, checked, position, created_at FROM result_checklist "
            "WHERE result_id = ? ORDER BY position ASC",
            (str(result_id),),
        ) as cur:
            rows = await cur.fetchall()
    return [{"id": r[0], "text": r[1], "checked": bool(r[2]), "position": r[3], "created_at": r[4]} for r in rows]


async def set_checklist_item_checked(result_id: UUID, item_id: int, checked: bool) -> bool:
    """Toggle checked state of a checklist item. Returns False if item not found."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_CHECKLIST)
        cursor = await db.execute(
            "UPDATE result_checklist SET checked = ? WHERE id = ? AND result_id = ?",
            (1 if checked else 0, item_id, str(result_id)),
        )
        await db.commit()
    return (cursor.rowcount or 0) > 0


async def delete_checklist_item(result_id: UUID, item_id: int) -> bool:
    """Delete a checklist item. Returns False if not found."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_CHECKLIST)
        cursor = await db.execute(
            "DELETE FROM result_checklist WHERE id = ? AND result_id = ?",
            (item_id, str(result_id)),
        )
        await db.commit()
    return (cursor.rowcount or 0) > 0


async def get_checklist_progress(result_id: UUID) -> dict:
    """Return total/checked/unchecked counts for a result's checklist."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_CHECKLIST)
        await db.commit()
        async with db.execute(
            "SELECT COUNT(*), SUM(checked) FROM result_checklist WHERE result_id = ?",
            (str(result_id),),
        ) as cur:
            row = await cur.fetchone()
    total = row[0] or 0
    checked = int(row[1] or 0)
    return {"total": total, "checked": checked, "unchecked": total - checked}


# ---------------------------------------------------------------------------
# Result reactions
# ---------------------------------------------------------------------------

async def add_reaction(result_id: UUID, emoji: str) -> dict:
    """Increment (or initialise) a reaction counter. Returns updated counts for the result."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_REACTIONS)
        await db.execute(
            """INSERT INTO result_reactions (result_id, emoji, count) VALUES (?, ?, 1)
               ON CONFLICT(result_id, emoji) DO UPDATE SET count = count + 1""",
            (str(result_id), emoji),
        )
        await db.commit()
    return await get_reactions(result_id)


async def remove_reaction(result_id: UUID, emoji: str) -> dict:
    """Decrement a reaction counter (floor 0; removes row at zero). Returns updated counts."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_REACTIONS)
        await db.execute(
            "UPDATE result_reactions SET count = MAX(0, count - 1) "
            "WHERE result_id = ? AND emoji = ?",
            (str(result_id), emoji),
        )
        await db.execute(
            "DELETE FROM result_reactions WHERE result_id = ? AND emoji = ? AND count = 0",
            (str(result_id), emoji),
        )
        await db.commit()
    return await get_reactions(result_id)


async def get_reactions(result_id: UUID) -> dict:
    """Return reaction counts keyed by emoji for a result."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_REACTIONS)
        await db.commit()
        async with db.execute(
            "SELECT emoji, count FROM result_reactions WHERE result_id = ? ORDER BY emoji",
            (str(result_id),),
        ) as cur:
            rows = await cur.fetchall()
    return {r[0]: r[1] for r in rows}


# ---------------------------------------------------------------------------
# Processing summary dashboard
# ---------------------------------------------------------------------------

async def get_processing_summary() -> dict:
    """Rich aggregate: flags, priority/workflow breakdown, top tags/labels, time windows."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_RESULTS)
        await db.execute(_DDL_TAGS)
        await db.execute(_DDL_LABELS)
        await db.execute(_DDL_RESULT_LABELS)
        await db.commit()

        async with db.execute("SELECT COUNT(*) FROM results") as cur:
            total: int = (await cur.fetchone())[0]  # type: ignore[index]

        async with db.execute(
            "SELECT priority, COUNT(*) FROM results GROUP BY priority"
        ) as cur:
            by_priority = {(r[0] or "none"): r[1] for r in await cur.fetchall()}

        async with db.execute(
            "SELECT workflow_status, COUNT(*) FROM results GROUP BY workflow_status"
        ) as cur:
            by_workflow = {(r[0] or "none"): r[1] for r in await cur.fetchall()}

        async with db.execute("SELECT SUM(starred), SUM(pinned), SUM(locked) FROM results") as cur:
            row = await cur.fetchone()
            flag_counts = {
                "starred": int(row[0] or 0),
                "pinned": int(row[1] or 0),
                "locked": int(row[2] or 0),
            }

        async with db.execute(
            "SELECT COUNT(*) FROM results WHERE expires_at IS NOT NULL "
            "AND expires_at <= strftime('%Y-%m-%dT%H:%M:%SZ', 'now')"
        ) as cur:
            expired_count: int = (await cur.fetchone())[0]  # type: ignore[index]

        async with db.execute(
            "SELECT tag, COUNT(*) AS cnt FROM tags GROUP BY tag ORDER BY cnt DESC LIMIT 10"
        ) as cur:
            top_tags = [{"tag": r[0], "count": r[1]} for r in await cur.fetchall()]

        async with db.execute(
            """SELECT l.name, l.color, COUNT(rl.result_id) AS cnt
               FROM labels l LEFT JOIN result_labels rl ON l.name = rl.label_name
               GROUP BY l.name ORDER BY cnt DESC LIMIT 10"""
        ) as cur:
            top_labels = [{"name": r[0], "color": r[1], "count": r[2]} for r in await cur.fetchall()]

        windows = {}
        for label, days in (("last_24h", 1), ("last_7d", 7), ("last_30d", 30)):
            async with db.execute(
                "SELECT COUNT(*) FROM results WHERE created_at >= "
                f"strftime('%Y-%m-%dT%H:%M:%SZ', datetime('now', '-{days} day'))"
            ) as cur:
                windows[label] = (await cur.fetchone())[0]  # type: ignore[index]

    return {
        "total": total,
        "expired": expired_count,
        "flags": flag_counts,
        "by_priority": by_priority,
        "by_workflow_status": by_workflow,
        "top_tags": top_tags,
        "top_labels": top_labels,
        "processed_windows": windows,
    }


# ---------------------------------------------------------------------------
# Result snapshots
# ---------------------------------------------------------------------------

async def create_snapshot(result_id: UUID, label: str | None = None) -> dict | None:
    """Save a snapshot of the current result data. Returns None if result doesn't exist."""
    _ensure_dir()
    result = await get_result(result_id)
    if result is None:
        return None
    data_json = result.model_dump_json()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_SNAPSHOTS)
        cursor = await db.execute(
            "INSERT INTO result_snapshots (result_id, label, data) VALUES (?, ?, ?)",
            (str(result_id), label, data_json),
        )
        snap_id = cursor.lastrowid
        await db.commit()
        async with db.execute(
            "SELECT id, result_id, label, created_at FROM result_snapshots WHERE id = ?",
            (snap_id,),
        ) as cur:
            row = await cur.fetchone()
    return {
        "id": row[0],
        "result_id": row[1],
        "label": row[2],
        "created_at": row[3],
    }


async def list_snapshots(result_id: UUID) -> list[dict]:
    """Return all snapshots for a result, newest first (without inline data)."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_SNAPSHOTS)
        await db.commit()
        async with db.execute(
            """SELECT id, result_id, label, created_at
               FROM result_snapshots WHERE result_id = ?
               ORDER BY id DESC""",
            (str(result_id),),
        ) as cur:
            rows = await cur.fetchall()
    return [{"id": r[0], "result_id": r[1], "label": r[2], "created_at": r[3]} for r in rows]


async def get_snapshot(result_id: UUID, snapshot_id: int) -> dict | None:
    """Fetch a single snapshot (including full data). Returns None if not found or wrong result."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_SNAPSHOTS)
        await db.commit()
        async with db.execute(
            """SELECT id, result_id, label, data, created_at
               FROM result_snapshots WHERE id = ? AND result_id = ?""",
            (snapshot_id, str(result_id)),
        ) as cur:
            row = await cur.fetchone()
    if not row:
        return None
    return {
        "id": row[0],
        "result_id": row[1],
        "label": row[2],
        "data": json.loads(row[3]),
        "created_at": row[4],
    }


# ---------------------------------------------------------------------------
# Result relations
# ---------------------------------------------------------------------------

RELATION_TYPES = {"related_to", "duplicate_of", "supersedes", "attachment_of"}


async def add_relation(source_id: UUID, target_id: UUID, relation: str) -> dict | None:
    """Add a typed relation. Returns the created entry, or None if it already exists."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_RELATIONS)
        try:
            cursor = await db.execute(
                "INSERT INTO result_relations (source_id, target_id, relation) VALUES (?, ?, ?)",
                (str(source_id), str(target_id), relation),
            )
            row_id = cursor.lastrowid
            await db.commit()
        except Exception:
            return None  # UNIQUE constraint violated
        async with db.execute(
            "SELECT id, source_id, target_id, relation, created_at FROM result_relations WHERE id = ?",
            (row_id,),
        ) as cur:
            row = await cur.fetchone()
    if row is None:
        return None
    return {"id": row[0], "source_id": row[1], "target_id": row[2],
            "relation": row[3], "created_at": row[4]}


async def get_relations(result_id: UUID) -> list[dict]:
    """Return all relations where result_id is source or target."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_RELATIONS)
        await db.commit()
        async with db.execute(
            """SELECT id, source_id, target_id, relation, created_at
               FROM result_relations
               WHERE source_id = ? OR target_id = ?
               ORDER BY id ASC""",
            (str(result_id), str(result_id)),
        ) as cur:
            rows = await cur.fetchall()
    return [
        {"id": r[0], "source_id": r[1], "target_id": r[2],
         "relation": r[3], "created_at": r[4]}
        for r in rows
    ]


async def delete_relation(result_id: UUID, relation_id: int) -> bool:
    """Delete a relation that involves result_id (as source or target)."""
    _ensure_dir()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(_DDL_RELATIONS)
        cursor = await db.execute(
            "DELETE FROM result_relations WHERE id = ? AND (source_id = ? OR target_id = ?)",
            (relation_id, str(result_id), str(result_id)),
        )
        await db.commit()
    return (cursor.rowcount or 0) > 0
