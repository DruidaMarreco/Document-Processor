from __future__ import annotations

MIME_TO_ROUTE: dict[str, str] = {
    "application/pdf": "pdf",
    # Images
    "image/jpeg": "image",
    "image/png": "image",
    "image/tiff": "image",
    "image/gif": "image",
    "image/webp": "image",
    "image/bmp": "image",
    "image/svg+xml": "image",
    "image/heic": "image",
    "image/heif": "image",
    "image/avif": "image",
    # Text
    "text/plain": "text",
    "text/markdown": "text",
    "text/csv": "spreadsheet",
    "text/tab-separated-values": "spreadsheet",
    "text/html": "html",
    "text/xml": "xml",
    "text/yaml": "yaml",
    "application/yaml": "yaml",
    "application/x-yaml": "yaml",
    # Structured data
    "application/json": "json",
    "application/x-ndjson": "json",
    "application/xml": "xml",
    # Office
    "application/msword": "word",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "word",
    "application/vnd.ms-excel": "spreadsheet",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "spreadsheet",
    "application/vnd.ms-powerpoint": "presentation",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation": "presentation",
    # OpenDocument
    "application/vnd.oasis.opendocument.text": "word",
    "application/vnd.oasis.opendocument.spreadsheet": "spreadsheet",
    "application/vnd.oasis.opendocument.presentation": "presentation",
    # Archives
    "application/zip": "archive",
    "application/x-tar": "archive",
    "application/gzip": "archive",
    "application/x-gzip": "archive",
    "application/x-bzip2": "archive",
    "application/x-xz": "archive",
    "application/x-rar-compressed": "archive",
    "application/vnd.rar": "archive",
    "application/x-7z-compressed": "archive",
    # Email
    "message/rfc822": "email",
    "application/vnd.ms-outlook": "email",
}

EXT_TO_ROUTE: dict[str, str] = {
    ".pdf": "pdf",
    # Images
    ".jpg": "image", ".jpeg": "image", ".png": "image",
    ".tiff": "image", ".tif": "image", ".gif": "image",
    ".webp": "image", ".bmp": "image", ".svg": "image",
    ".heic": "image", ".heif": "image", ".avif": "image",
    # Text / markup
    ".txt": "text", ".md": "text", ".markdown": "text", ".rst": "text",
    # Spreadsheets
    ".csv": "spreadsheet", ".tsv": "spreadsheet",
    ".xls": "spreadsheet", ".xlsx": "spreadsheet",
    ".ods": "spreadsheet", ".numbers": "spreadsheet",
    # HTML
    ".html": "html", ".htm": "html", ".xhtml": "html",
    # Data formats
    ".json": "json", ".jsonl": "json", ".ndjson": "json",
    ".xml": "xml",
    ".yaml": "yaml", ".yml": "yaml",
    # Word processing
    ".doc": "word", ".docx": "word", ".odt": "word", ".rtf": "word", ".pages": "word",
    # Presentations
    ".ppt": "presentation", ".pptx": "presentation",
    ".odp": "presentation", ".key": "presentation",
    # Archives
    ".zip": "archive", ".tar": "archive", ".gz": "archive",
    ".rar": "archive", ".7z": "archive", ".bz2": "archive", ".xz": "archive",
    # Email
    ".eml": "email", ".msg": "email", ".mbox": "email",
}

# (magic_bytes, internal_route_key)
# Checked in order — more specific signatures first
MAGIC_SIGNATURES: list[tuple[bytes, str]] = [
    (b"%PDF", "pdf"),
    (b"\xff\xd8\xff", "image"),           # JPEG
    (b"\x89PNG\r\n\x1a\n", "image"),     # PNG
    (b"GIF87a", "image"),                  # GIF
    (b"GIF89a", "image"),
    (b"II*\x00", "image"),                 # TIFF LE
    (b"MM\x00*", "image"),                 # TIFF BE
    (b"BM", "image"),                      # BMP
    (b"RIFF", "riff"),                     # Could be WebP or WAV — disambiguate below
    (b"\x00\x00\x00\x0cftyp", "image"),   # HEIF/HEIC/AVIF container
    (b"PK\x03\x04", "zip_based"),          # ZIP / DOCX / XLSX / PPTX
    (b"\xd0\xcf\x11\xe0", "ole"),          # OLE2: DOC / XLS / PPT / MSG
    # Email — checked before generic text markers
    (b"From ", "email"),
    (b"MIME-Version:", "email"),
    (b"Return-Path:", "email"),
    (b"Received:", "email"),
    (b"X-Mailer:", "email"),
    (b"Message-ID:", "email"),
    # HTML (handle optional UTF-8 BOM prefix via _magic_key)
    (b"<!DOCTYPE html", "html"),
    (b"<!doctype html", "html"),
    (b"<html", "html"),
    # Structured data
    (b"<?xml", "xml"),
    (b"---\n", "yaml"),                    # YAML document start
    (b"---\r\n", "yaml"),
    (b"%YAML", "yaml"),
    (b"{", "_maybe_json"),
    (b"[", "_maybe_json"),
    # Compression
    (b"\x1f\x8b", "archive"),             # gzip
    (b"BZh", "archive"),                  # bzip2
    (b"\xfd7zXZ\x00", "archive"),         # xz
    (b"Rar!\x1a\x07", "archive"),         # RAR
    (b"7z\xbc\xaf'\x1c", "archive"),      # 7-zip
    (b"\x1f\x9d", "archive"),             # compress (.Z)
]

# Names present inside a ZIP that identify the format
ZIP_INDICATORS: list[tuple[str, str]] = [
    ("word/", "word"),
    ("xl/", "spreadsheet"),
    ("ppt/", "presentation"),
    ("content.xml", "word"),              # OpenDocument (ODF)
    ("META-INF/", "archive"),             # generic OpenDocument / JAR
]

OLE_EXT_TO_ROUTE: dict[str, str] = {
    ".doc": "word",
    ".xls": "spreadsheet",
    ".ppt": "presentation",
    ".msg": "email",
    ".mpp": "generic",
    ".pub": "generic",
}

# CSV/TSV sniff: minimum fraction of rows that must have consistent delimiters
_CSV_MIN_ROWS = 3
_CSV_MIN_CONSISTENCY = 0.75
