from __future__ import annotations

import time
from pathlib import Path

from document_processor.models import Document, StageResult

from classifier_module.rules import ROUTE_TO_TYPE, TYPE_KEYWORDS


class ClassifierModule:
    name = "classifier"

    async def process(self, document: Document, context: dict) -> StageResult:
        t = time.monotonic()
        route = context.get("router", {}).get("route", "generic")
        doc_type, confidence, method = self._classify(document, route)
        return StageResult(
            module=self.name,
            status="success",
            data={"type": doc_type, "confidence": confidence, "method": method},
            duration_ms=(time.monotonic() - t) * 1000,
        )

    async def health_check(self) -> bool:
        return True

    def _classify(self, document: Document, route: str) -> tuple[str, float, str]:
        # Non-text routes are unambiguous from the route alone
        base = ROUTE_TO_TYPE.get(route)
        if base:
            return base, 0.90, "route"

        text = self._readable_text(document)

        if text:
            doc_type, score = self._keyword_score(text)
            if score > 0:
                # Each matching keyword adds ~0.08 confidence, capped at 0.92
                return doc_type, min(0.44 + score * 0.08, 0.92), "keyword"

        # Filename hint
        if document.filename:
            stem = Path(document.filename).stem.lower()
            for doc_type, keywords in TYPE_KEYWORDS:
                if any(kw.strip() in stem for kw in keywords):
                    return doc_type, 0.55, "filename"

        # Generic fallback based on route
        if route in ("pdf", "word"):
            return "document", 0.35, "route"

        return "unknown", 0.10, "fallback"

    def _readable_text(self, document: Document) -> str:
        try:
            return document.content.decode("utf-8", errors="ignore").lower()
        except Exception:
            return ""

    def _keyword_score(self, text: str) -> tuple[str, int]:
        best_type, best_score = "unknown", 0
        for doc_type, keywords in TYPE_KEYWORDS:
            score = sum(1 for kw in keywords if kw in text)
            if score > best_score:
                best_type, best_score = doc_type, score
        return best_type, best_score
