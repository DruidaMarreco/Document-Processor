from __future__ import annotations

import time
from pathlib import Path

from document_processor.config import settings
from document_processor.models import Document, StageResult

from classifier_module.rules import (
    FILENAME_PATTERNS,
    ROUTE_TO_TYPE,
    TYPE_KEYWORDS,
    TYPE_KEYWORDS_WEIGHTED,
    _HIGH_WEIGHT,
    _LOW_WEIGHT,
    _MAX_SCORE,
)

_ALL_TYPES = [t for t, _, _ in TYPE_KEYWORDS_WEIGHTED] + list(ROUTE_TO_TYPE.values()) + ["unknown"]

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
            result = self._weighted_keyword_score(text)
            if result is not None:
                doc_type, confidence = result
                return doc_type, confidence, "keyword"

        # Filename-based matching
        if document.filename:
            stem = Path(document.filename).stem.lower()
            for doc_type, patterns in FILENAME_PATTERNS:
                if any(pat in stem for pat in patterns):
                    return doc_type, 0.55, "filename"
            # Fallback: any keyword in stem
            for doc_type, keywords in TYPE_KEYWORDS:
                if any(kw.strip() in stem for kw in keywords):
                    return doc_type, 0.45, "filename"

        if route in ("pdf", "word", "text"):
            return "document", 0.35, "route"

        return "unknown", 0.10, "fallback"

    def _readable_text(self, document: Document) -> str:
        try:
            return document.content.decode("utf-8", errors="ignore").lower()
        except Exception:
            return ""

    def _weighted_keyword_score(self, text: str) -> tuple[str, float] | None:
        """Return (doc_type, confidence) using weighted keyword hits, or None if no match."""
        scores: dict[str, int] = {}
        for doc_type, high_kws, low_kws in TYPE_KEYWORDS_WEIGHTED:
            score = (
                sum(_HIGH_WEIGHT for kw in high_kws if kw in text)
                + sum(_LOW_WEIGHT for kw in low_kws if kw in text)
            )
            if score > 0:
                scores[doc_type] = score

        if not scores:
            return None

        # Sort by raw score descending
        ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        best_type, best_score = ranked[0]
        second_score = ranked[1][1] if len(ranked) > 1 else 0

        max_possible = _MAX_SCORE.get(best_type, 1)
        # Normalised hit rate: fraction of max possible score achieved
        hit_rate = min(best_score / max_possible, 1.0)

        # Base confidence from hit rate (scales between 0.45 and 0.92)
        confidence = 0.45 + hit_rate * 0.47

        # Separation bonus: boost when winner clearly leads runner-up
        if best_score > 0:
            separation = (best_score - second_score) / best_score
            confidence = min(0.95, confidence + separation * 0.12)

        # Penalty when the score is very low (1–2 raw points)
        if best_score <= _LOW_WEIGHT:
            confidence *= 0.70

        return best_type, round(confidence, 3)

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
