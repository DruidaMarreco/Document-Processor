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
    _CSV_MIN_CONSISTENCY,
    _CSV_MIN_ROWS,
)

# UTF-8 / UTF-16 byte-order marks to strip before magic matching
_BOMS = (
    b"\xef\xbb\xbf",   # UTF-8 BOM
    b"\xff\xfe",        # UTF-16 LE BOM
    b"\xfe\xff",        # UTF-16 BE BOM
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
        mime_route = MIME_TO_ROUTE.get(document.mimetype)
        ext_route: str | None = None
        if document.filename:
            ext = Path(document.filename).suffix.lower()
            ext_route = EXT_TO_ROUTE.get(ext)

        # --- ZIP-based formats (DOCX, XLSX, PPTX, ODF, …) ---
        if magic_key == "zip_based":
            r = self._inspect_zip(document.content)
            if r:
                confidence = 0.97 if (mime_route == r or ext_route == r) else 0.95
                return r, confidence, "zip_content"
            return "archive", 0.88, "magic_bytes"

        # --- OLE2 (DOC, XLS, PPT, MSG) ---
        if magic_key == "ole":
            if document.filename:
                ext = Path(document.filename).suffix.lower()
                r = OLE_EXT_TO_ROUTE.get(ext)
                if r:
                    confidence = 0.95 if mime_route == r else 0.92
                    return r, confidence, "ole_extension"
            if mime_route and mime_route in ("word", "spreadsheet", "presentation", "email"):
                return mime_route, 0.88, "ole_mime"
            return "word", 0.65, "ole_magic"

        # --- RIFF (WebP, WAV, AVI, …) ---
        if magic_key == "riff":
            if len(document.content) >= 12 and document.content[8:12] == b"WEBP":
                return "image", 0.97, "magic_bytes"
            return "generic", 0.40, "magic_bytes"

        # --- HEIF/HEIC/AVIF container ---
        if magic_key == "image" and len(document.content) >= 12:
            ftyp = document.content[8:12]
            if ftyp in (b"heic", b"heix", b"hevc", b"hevx", b"mif1", b"avif"):
                return "image", 0.95, "magic_bytes"

        # --- Tentative JSON (starts with { or [) ---
        if magic_key == "_maybe_json":
            if self._valid_json(document.content):
                route = "json"
                confidence = 0.90 if (mime_route == "json" or ext_route == "json") else 0.82
                return route, confidence, "magic_bytes"
            # Could be YAML or text — fall through to other signals

        # --- Clear magic match ---
        if magic_key and not magic_key.startswith("_"):
            confidence = 0.97 if (mime_route == magic_key or ext_route == magic_key) else 0.95
            return magic_key, confidence, "magic_bytes"

        # --- Declared MIME type ---
        if mime_route:
            confidence = 0.80 if ext_route == mime_route else 0.75
            # Extra: content-sniff text/plain as possible CSV/TSV
            if mime_route == "text" and document.content:
                sniffed = self._sniff_delimited(document.content)
                if sniffed:
                    return sniffed, 0.72, "content_sniff"
            return mime_route, confidence, "declared_mimetype"

        # --- File extension ---
        if ext_route:
            # Content-sniff text routes for CSV/TSV before committing
            if ext_route == "text" and document.content:
                sniffed = self._sniff_delimited(document.content)
                if sniffed:
                    return sniffed, 0.65, "content_sniff"
            return ext_route, 0.55, "file_extension"

        # --- Last-ditch content sniff ---
        if document.content:
            sniffed = self._sniff_delimited(document.content)
            if sniffed:
                return sniffed, 0.50, "content_sniff"

        return "generic", 0.0, "fallback"

    def _magic_key(self, content: bytes) -> str | None:
        if not content:
            return None
        # Strip leading BOM before comparing
        stripped = content
        for bom in _BOMS:
            if content.startswith(bom):
                stripped = content[len(bom):]
                break
        for sig, key in MAGIC_SIGNATURES:
            candidate = stripped if sig[:1].isascii() else content
            if sig in (b"<!DOCTYPE html", b"<!doctype html", b"<html"):
                if candidate[: len(sig)].lower() == sig.lower():
                    return key
            elif candidate[: len(sig)] == sig:
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
            # Try NDJSON (newline-delimited JSON)
            try:
                lines = content.strip().split(b"\n")
                if len(lines) >= 1:
                    for line in lines[:5]:
                        json.loads(line.strip())
                    return True
            except Exception:
                pass
        return False

    def _sniff_delimited(self, content: bytes) -> str | None:
        """Return 'spreadsheet' when content looks like CSV or TSV."""
        try:
            text = content[:4096].decode("utf-8", errors="replace")
        except Exception:
            return None
        lines = [ln for ln in text.splitlines() if ln.strip()]
        if len(lines) < _CSV_MIN_ROWS:
            return None

        for delimiter, min_per_row in (("\t", 1), (",", 1)):
            counts = [line.count(delimiter) for line in lines]
            consistent = sum(1 for c in counts if c >= min_per_row)
            if consistent / len(counts) >= _CSV_MIN_CONSISTENCY:
                return "spreadsheet"
        return None
