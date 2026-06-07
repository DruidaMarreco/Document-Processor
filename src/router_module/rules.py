from __future__ import annotations

MIME_TO_ROUTE: dict[str, str] = {
    "application/pdf": "pdf",
    "image/jpeg": "image",
    "image/png": "image",
    "image/tiff": "image",
    "image/gif": "image",
    "image/webp": "image",
    "image/bmp": "image",
    "image/svg+xml": "image",
    "text/plain": "text",
    "text/csv": "spreadsheet",
    "text/html": "html",
    "text/xml": "xml",
    "application/json": "json",
    "application/xml": "xml",
    "application/msword": "word",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "word",
    "application/vnd.ms-excel": "spreadsheet",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "spreadsheet",
    "application/vnd.ms-powerpoint": "presentation",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation": "presentation",
    "application/zip": "archive",
    "application/x-tar": "archive",
    "application/gzip": "archive",
    "application/x-rar-compressed": "archive",
    "message/rfc822": "email",
}

EXT_TO_ROUTE: dict[str, str] = {
    ".pdf": "pdf",
    ".jpg": "image", ".jpeg": "image", ".png": "image",
    ".tiff": "image", ".tif": "image", ".gif": "image",
    ".webp": "image", ".bmp": "image", ".svg": "image",
    ".txt": "text", ".md": "text",
    ".csv": "spreadsheet", ".xls": "spreadsheet", ".xlsx": "spreadsheet",
    ".html": "html", ".htm": "html",
    ".json": "json",
    ".xml": "xml",
    ".doc": "word", ".docx": "word", ".odt": "word", ".rtf": "word",
    ".ppt": "presentation", ".pptx": "presentation", ".odp": "presentation",
    ".zip": "archive", ".tar": "archive", ".gz": "archive", ".rar": "archive", ".7z": "archive",
    ".eml": "email", ".msg": "email",
}

# (magic_bytes, internal_route_key)
# Checked in order — put more specific signatures first
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
    (b"PK\x03\x04", "zip_based"),          # ZIP / DOCX / XLSX / PPTX
    (b"\xd0\xcf\x11\xe0", "ole"),          # OLE2: DOC / XLS / PPT
    (b"From ", "email"),
    (b"MIME-Version:", "email"),
    (b"Return-Path:", "email"),
    (b"<!DOCTYPE html", "html"),
    (b"<!doctype html", "html"),
    (b"<html", "html"),
    (b"<?xml", "xml"),
    (b"{", "_maybe_json"),
    (b"[", "_maybe_json"),
]

# Names present inside a ZIP that identify the format
ZIP_INDICATORS: list[tuple[str, str]] = [
    ("word/", "word"),
    ("xl/", "spreadsheet"),
    ("ppt/", "presentation"),
    ("META-INF/", "archive"),           # generic OpenDocument / JAR
]

OLE_EXT_TO_ROUTE: dict[str, str] = {
    ".doc": "word",
    ".xls": "spreadsheet",
    ".ppt": "presentation",
    ".msg": "email",
}
