from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, HttpUrl


class Document(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    filename: str | None = None
    mimetype: str = "application/octet-stream"
    content: bytes
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    model_config = {"arbitrary_types_allowed": True}


class StageResult(BaseModel):
    module: str
    status: Literal["success", "error", "skipped"]
    data: dict[str, Any] = Field(default_factory=dict)
    errors: list[str] = Field(default_factory=list)
    duration_ms: float


class PipelineResult(BaseModel):
    document_id: UUID
    status: Literal["success", "partial", "failed"]
    stages: list[StageResult] = Field(default_factory=list)
    output: dict[str, Any] = Field(default_factory=dict)
    total_duration_ms: float
    completed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


WebhookEvent = Literal["document.processed", "document.failed", "document.reprocessed"]


class WebhookConfig(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    url: str
    events: list[str] = Field(default_factory=lambda: ["document.processed", "document.failed"])
    doc_types: list[str] = Field(default_factory=list, description="If non-empty, only fire for these doc_types")
    secret: str | None = None
    active: bool = True
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class ApiKey(BaseModel):
    key: str  # full key — only returned at creation time
    name: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class ApiKeyInfo(BaseModel):
    prefix: str  # first 8 chars, safe to display
    name: str
    created_at: datetime


class JobRecord(BaseModel):
    job_id: UUID = Field(default_factory=uuid4)
    status: Literal["queued", "processing", "done", "failed"]
    filename: str | None = None
    mimetype: str = "application/octet-stream"
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    started_at: datetime | None = None
    completed_at: datetime | None = None
    result_id: UUID | None = None
    error: str | None = None
