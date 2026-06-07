"""
Stub implementations for each module.

These are used until the real module packages are installed.
To replace a stub, install the module package and update registry.py.
"""

import time

from document_processor.models import Document, StageResult


def _elapsed(start: float) -> float:
    return (time.monotonic() - start) * 1000


def _route(document: Document) -> str:
    mt = document.mimetype
    if mt == "application/pdf":
        return "pdf"
    if mt.startswith("image/"):
        return "image"
    if mt in ("text/html",):
        return "html"
    if mt in ("text/csv", "application/vnd.ms-excel", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"):
        return "spreadsheet"
    if mt in ("application/msword", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"):
        return "word"
    return "generic"


class RouterModuleStub:
    name = "router"

    async def process(self, document: Document, context: dict) -> StageResult:
        t = time.monotonic()
        return StageResult(
            module=self.name, status="success",
            data={"route": _route(document), "stub": True},
            duration_ms=_elapsed(t),
        )

    async def health_check(self) -> bool:
        return True


class ClassifierModuleStub:
    name = "classifier"

    async def process(self, document: Document, context: dict) -> StageResult:
        t = time.monotonic()
        route = context.get("router", {}).get("route", "generic")
        doc_type = {
            "pdf": "document", "image": "image",
            "html": "webpage", "spreadsheet": "spreadsheet",
            "word": "document",
        }.get(route, "unknown")
        return StageResult(
            module=self.name, status="success",
            data={"type": doc_type, "confidence": 0.0, "stub": True},
            duration_ms=_elapsed(t),
        )

    async def health_check(self) -> bool:
        return True


class ExtractorModuleStub:
    name = "extractor"

    async def process(self, document: Document, context: dict) -> StageResult:
        t = time.monotonic()
        return StageResult(
            module=self.name, status="success",
            data={"fields": {}, "raw_text": "", "stub": True},
            duration_ms=_elapsed(t),
        )

    async def health_check(self) -> bool:
        return True


class RefinerModuleStub:
    name = "refiner"

    async def process(self, document: Document, context: dict) -> StageResult:
        t = time.monotonic()
        fields = context.get("extractor", {}).get("fields", {})
        return StageResult(
            module=self.name, status="success",
            data={"fields": fields, "stub": True},
            duration_ms=_elapsed(t),
        )

    async def health_check(self) -> bool:
        return True


class ValidatorModuleStub:
    name = "validator"

    async def process(self, document: Document, context: dict) -> StageResult:
        t = time.monotonic()
        return StageResult(
            module=self.name, status="success",
            data={"valid": True, "violations": [], "stub": True},
            duration_ms=_elapsed(t),
        )

    async def health_check(self) -> bool:
        return True


class GeneratorModuleStub:
    name = "generator"

    async def process(self, document: Document, context: dict) -> StageResult:
        t = time.monotonic()
        return StageResult(
            module=self.name, status="success",
            data={"output": {}, "format": "json", "stub": True},
            duration_ms=_elapsed(t),
        )

    async def health_check(self) -> bool:
        return True
