from __future__ import annotations

import time
from pathlib import Path

from document_processor.config import settings
from document_processor.models import Document, StageResult

from classifier_module.rules import ROUTE_TO_TYPE, TYPE_KEYWORDS

_ALL_TYPES = [t for t, _ in TYPE_KEYWORDS] + list(ROUTE_TO_TYPE.values()) + ["unknown"]

# LLM client created once; gracefully absent when ANTHROPIC_API_KEY is not set
_llm: object = None


def _get_llm():
    global _llm
    if _llm is None:
        from llm_client import LLMClient
        _llm = LLMClient(
            model=settings.llm_model,
            max_input_chars=settings.llm_max_input_chars,
        )
    return _llm


class ClassifierModule:
    name = "classifier"

    async def process(self, document: Document, context: dict) -> StageResult:
        t = time.monotonic()
        cfg: dict = context.get("_config") or {}
        route = context.get("router", {}).get("route", "generic")

        # Per-request forced doc_type skips all classification
        if cfg.get("force_doc_type"):
            return StageResult(
                module=self.name,
                status="success",
                data={"type": cfg["force_doc_type"], "confidence": 1.0, "method": "forced"},
                duration_ms=(time.monotonic() - t) * 1000,
            )

        doc_type, confidence, method = self._classify(document, route)

        llm_enabled = cfg.get("llm_enabled", settings.llm_enabled)
        threshold = cfg.get("confidence_threshold", settings.llm_confidence_threshold)
        if llm_enabled and confidence < threshold:
            doc_type, confidence, method = await self._llm_classify(
                document, route, doc_type, confidence, method
            )

        return StageResult(
            module=self.name,
            status="success",
            data={"type": doc_type, "confidence": confidence, "method": method},
            duration_ms=(time.monotonic() - t) * 1000,
        )

    async def health_check(self) -> bool:
        return True

    # ------------------------------------------------------------------
    # Heuristic classification
    # ------------------------------------------------------------------

    def _classify(self, document: Document, route: str) -> tuple[str, float, str]:
        base = ROUTE_TO_TYPE.get(route)
        if base:
            return base, 0.90, "route"

        text = self._readable_text(document)

        if text:
            doc_type, score = self._keyword_score(text)
            if score > 0:
                return doc_type, min(0.44 + score * 0.08, 0.92), "keyword"

        if document.filename:
            stem = Path(document.filename).stem.lower()
            for doc_type, keywords in TYPE_KEYWORDS:
                if any(kw.strip() in stem for kw in keywords):
                    return doc_type, 0.55, "filename"

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

    # ------------------------------------------------------------------
    # LLM fallback
    # ------------------------------------------------------------------

    async def _llm_classify(
        self,
        document: Document,
        route: str,
        fallback_type: str,
        fallback_confidence: float,
        fallback_method: str,
    ) -> tuple[str, float, str]:
        llm = _get_llm()
        if not llm.available:
            return fallback_type, fallback_confidence, fallback_method

        image_bytes: bytes | None = None
        image_media_type = "image/jpeg"
        if route == "image" and settings.llm_vision_enabled:
            image_bytes = document.content
            if document.mimetype:
                image_media_type = document.mimetype

        text = self._readable_text(document)

        result = await llm.classify(
            text=text,
            known_types=_ALL_TYPES,
            image_bytes=image_bytes,
            image_media_type=image_media_type,
        )
        if result is None:
            return fallback_type, fallback_confidence, fallback_method

        doc_type, confidence = result
        return doc_type, confidence, "llm"
