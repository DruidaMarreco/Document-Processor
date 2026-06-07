from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


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
