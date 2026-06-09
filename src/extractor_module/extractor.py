from __future__ import annotations

import csv
import io
import json
import time
import xml.etree.ElementTree as ET
import zipfile
from html.parser import HTMLParser
from pathlib import Path

from document_processor.config import settings
from document_processor.models import Document, StageResult

from extractor_module.patterns import FIELD_PATTERNS

_MAX_TEXT = 8_000  # chars kept in context for downstream stages

# Optional OCR dependencies — gracefully absent
try:
    import pdfplumber as _pdfplumber
except Exception:
    _pdfplumber = None

try:
    import pytesseract as _pytesseract
    from PIL import Image as _PILImage
except Exception:
    _pytesseract = None
    _PILImage = None

# Doc types where LLM extraction adds meaningful value beyond regex
_LLM_ENRICHED_TYPES = {
    "invoice", "receipt", "purchase_order", "contract",
    "statement", "bank_statement", "report", "specification", "letter",
    "nda", "tax_document", "payslip", "insurance", "medical_record",
    "quote", "delivery_note", "form", "resume",
}

# DOCX word/document.xml namespace
_DOCX_NS = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}

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


class _StripHTML(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._parts: list[str] = []

    def handle_data(self, data: str) -> None:
        s = data.strip()
        if s:
            self._parts.append(s)

    @property
    def text(self) -> str:
        return " ".join(self._parts)


def _decode(content: bytes) -> str:
    """Decode bytes trying UTF-8 first, then latin-1 as fallback."""
    try:
        return content.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        return content.decode("latin-1", errors="ignore")


class ExtractorModule:
    name = "extractor"

    async def process(self, document: Document, context: dict) -> StageResult:
        t = time.monotonic()
        route = context.get("router", {}).get("route", "generic")
        doc_type = context.get("classifier", {}).get("type", "unknown")

        raw_text, structured = self._extract(document, route)
        fields = self._pattern_fields(raw_text) if raw_text else {}
        fields.update(structured)

        if settings.llm_enabled and (
            doc_type in _LLM_ENRICHED_TYPES or route == "image"
        ):
            fields = await self._llm_enrich(document, route, doc_type, raw_text, fields)

        metadata = self._build_metadata(document, route, raw_text)

        return StageResult(
            module=self.name,
            status="success",
            data={
                "raw_text": raw_text[:_MAX_TEXT],
                "fields": fields,
                "metadata": metadata,
            },
            duration_ms=(time.monotonic() - t) * 1000,
        )

    async def health_check(self) -> bool:
        return True

    # ------------------------------------------------------------------
    # Route-based extraction
    # ------------------------------------------------------------------

    def _extract(self, document: Document, route: str) -> tuple[str, dict]:
        if route == "json":
            return self._from_json(document.content)
        if route == "yaml":
            return self._from_yaml(document.content)
        if route == "xml":
            return self._from_xml(document.content)
        if route == "spreadsheet":
            return self._from_spreadsheet(document)
        if route == "html":
            return self._from_html(document.content), {}
        if route == "pdf":
            return self._from_pdf(document.content), {}
        if route == "image":
            return self._from_image_ocr(document.content), {}
        if route == "word":
            return self._from_word(document.content), {}
        if route == "email":
            return self._from_email(document.content)
        return self._from_text(document.content), {}

    def _from_json(self, content: bytes) -> tuple[str, dict]:
        try:
            data = json.loads(content)
            return "", {"json_data": data}
        except Exception:
            # Try NDJSON
            try:
                lines = _decode(content).strip().splitlines()
                records = [json.loads(ln) for ln in lines if ln.strip()]
                if records:
                    return "", {"json_data": records}
            except Exception:
                pass
            return _decode(content), {}

    def _from_yaml(self, content: bytes) -> tuple[str, dict]:
        text = _decode(content)
        try:
            import yaml  # type: ignore
            data = yaml.safe_load(text)
            if isinstance(data, dict):
                return text, {"yaml_data": data}
        except Exception:
            pass
        return text, {}

    def _from_xml(self, content: bytes) -> tuple[str, dict]:
        text = _decode(content)
        try:
            root = ET.fromstring(content)
            # Collect all text nodes
            parts = [node.strip() for node in root.itertext() if node.strip()]
            return " ".join(parts), {}
        except Exception:
            return text, {}

    def _from_spreadsheet(self, document: Document) -> tuple[str, dict]:
        """Handle CSV and TSV; fall back to raw text for binary formats."""
        content = document.content
        filename = document.filename or ""
        mime = document.mimetype or ""

        # Detect delimiter from extension or sniff
        ext = Path(filename).suffix.lower()
        delimiter = "\t" if ext == ".tsv" or "tab-separated" in mime else None

        try:
            text = _decode(content)
            if delimiter is None:
                # Sniff the delimiter
                sample = text[:2048]
                try:
                    sniffed = csv.Sniffer().sniff(sample, delimiters=",\t|;")
                    delimiter = sniffed.delimiter
                except Exception:
                    delimiter = ","
            reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
            rows = list(reader)
            fieldnames = list(reader.fieldnames or [])
            return text, {
                "columns": fieldnames,
                "row_count": len(rows),
                "sample_rows": rows[:5],
                "delimiter": delimiter,
            }
        except Exception:
            return _decode(content), {}

    def _from_html(self, content: bytes) -> str:
        parser = _StripHTML()
        parser.feed(_decode(content))
        return parser.text

    def _from_text(self, content: bytes) -> str:
        return _decode(content)

    def _from_pdf(self, content: bytes) -> str:
        if _pdfplumber is not None:
            try:
                with _pdfplumber.open(io.BytesIO(content)) as pdf:
                    pages = [p.extract_text() or "" for p in pdf.pages]
                text = "\n".join(pages).strip()
                if text:
                    return text
            except Exception:
                pass
        return _decode(content)

    def _from_word(self, content: bytes) -> str:
        """Extract plain text from DOCX (ZIP + XML) or fall back to decode."""
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as zf:
                if "word/document.xml" in zf.namelist():
                    xml_bytes = zf.read("word/document.xml")
                    root = ET.fromstring(xml_bytes)
                    parts: list[str] = []
                    for elem in root.iter("{http://schemas.openxmlformats.org/wordprocessingml/2006/main}t"):
                        if elem.text:
                            parts.append(elem.text)
                    return " ".join(parts)
        except Exception:
            pass
        return _decode(content)

    def _from_image_ocr(self, content: bytes) -> str:
        if _pytesseract is not None and _PILImage is not None:
            try:
                img = _PILImage.open(io.BytesIO(content))
                return _pytesseract.image_to_string(img)
            except Exception:
                pass
        return ""

    def _from_email(self, content: bytes) -> tuple[str, dict]:
        """Parse basic email headers and body."""
        text = _decode(content)
        headers: dict[str, str] = {}
        body_lines: list[str] = []
        in_body = False
        for line in text.splitlines():
            if in_body:
                body_lines.append(line)
            elif line == "" or line == "\r":
                in_body = True
            else:
                if ":" in line:
                    key, _, val = line.partition(":")
                    key = key.strip().lower()
                    val = val.strip()
                    if key in ("from", "to", "subject", "date", "cc", "bcc", "reply-to", "message-id"):
                        headers[key] = val
        body = "\n".join(body_lines).strip()
        full_text = text if not body else body
        structured = {"email_headers": headers} if headers else {}
        return full_text, structured

    def _pattern_fields(self, text: str) -> dict:
        result: dict = {}
        for name, pattern in FIELD_PATTERNS.items():
            matches = pattern.findall(text)
            if matches:
                seen: set = set()
                result[name] = [m for m in matches if not (m in seen or seen.add(m))]
        return result

    def _build_metadata(self, document: Document, route: str, raw_text: str) -> dict:
        meta: dict = {
            "size_bytes": len(document.content),
            "filename": document.filename,
            "mimetype": document.mimetype,
            "char_count": len(raw_text),
        }
        # Page count for PDFs
        if route == "pdf" and _pdfplumber is not None:
            try:
                with _pdfplumber.open(io.BytesIO(document.content)) as pdf:
                    meta["page_count"] = len(pdf.pages)
            except Exception:
                pass
        return meta

    # ------------------------------------------------------------------
    # LLM enrichment
    # ------------------------------------------------------------------

    async def _llm_enrich(
        self,
        document: Document,
        route: str,
        doc_type: str,
        raw_text: str,
        existing_fields: dict,
    ) -> dict:
        llm = _get_llm()
        if not llm.available:
            return existing_fields

        image_bytes: bytes | None = None
        image_media_type = "image/jpeg"
        if route == "image" and settings.llm_vision_enabled:
            image_bytes = document.content
            if document.mimetype:
                image_media_type = document.mimetype

        llm_fields = await llm.extract(
            text=raw_text,
            doc_type=doc_type,
            image_bytes=image_bytes,
            image_media_type=image_media_type,
        )
        if not llm_fields:
            return existing_fields

        merged = dict(existing_fields)
        for key, llm_values in llm_fields.items():
            if key not in merged:
                merged[key] = llm_values
            else:
                existing = merged[key]
                if isinstance(existing, list) and isinstance(llm_values, list):
                    seen: set = set(map(str, existing))
                    extras = [v for v in llm_values if str(v) not in seen]
                    merged[key] = existing + extras
        return merged
