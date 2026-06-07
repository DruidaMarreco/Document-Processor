from __future__ import annotations

import io
import json
import time
import zipfile
from pathlib import Path

from document_processor.models import Document, StageResult

from router_module.rules import (
    EXT_TO_ROUTE,
    MAGIC_SIGNATURES,
    MIME_TO_ROUTE,
    OLE_EXT_TO_ROUTE,
    ZIP_INDICATORS,
)


class RouterModule:
    name = "router"

    async def process(self, document: Document, context: dict) -> StageResult:
        t = time.monotonic()
        route, confidence, method = self._route(document)
        return StageResult(
            module=self.name,
            status="success",
            data={
                "route": route,
                "confidence": confidence,
                "method": method,
                "mimetype_declared": document.mimetype,
            },
            duration_ms=(time.monotonic() - t) * 1000,
        )

    async def health_check(self) -> bool:
        return True

    # ------------------------------------------------------------------
    # Internal routing logic
    # ------------------------------------------------------------------

    def _route(self, document: Document) -> tuple[str, float, str]:
        magic_key = self._magic_key(document.content)

        if magic_key == "zip_based":
            r = self._inspect_zip(document.content)
            if r:
                return r, 0.95, "zip_content"
            return "archive", 0.88, "magic_bytes"

        if magic_key == "ole":
            if document.filename:
                ext = Path(document.filename).suffix.lower()
                r = OLE_EXT_TO_ROUTE.get(ext)
                if r:
                    return r, 0.92, "ole_extension"
            return "word", 0.65, "ole_magic"

        if magic_key == "riff":
            if len(document.content) >= 12 and document.content[8:12] == b"WEBP":
                return "image", 0.95, "magic_bytes"
            return "generic", 0.40, "magic_bytes"

        if magic_key == "_maybe_json":
            if self._valid_json(document.content):
                return "json", 0.82, "magic_bytes"

        if magic_key and not magic_key.startswith("_"):
            return magic_key, 0.95, "magic_bytes"

        # Declared MIME type
        r = MIME_TO_ROUTE.get(document.mimetype)
        if r:
            return r, 0.75, "declared_mimetype"

        # File extension
        if document.filename:
            ext = Path(document.filename).suffix.lower()
            r = EXT_TO_ROUTE.get(ext)
            if r:
                return r, 0.55, "file_extension"

        return "generic", 0.0, "fallback"

    def _magic_key(self, content: bytes) -> str | None:
        for sig, key in MAGIC_SIGNATURES:
            if content[: len(sig)].lower() == sig.lower() if sig in (
                b"<!DOCTYPE html", b"<!doctype html", b"<html"
            ) else content[: len(sig)] == sig:
                return key
        return None

    def _inspect_zip(self, content: bytes) -> str | None:
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as zf:
                names = zf.namelist()
            for name in names:
                for prefix, route in ZIP_INDICATORS:
                    if name.startswith(prefix):
                        return route
        except Exception:
            pass
        return None

    def _valid_json(self, content: bytes) -> bool:
        try:
            json.loads(content)
            return True
        except Exception:
            return False
