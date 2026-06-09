"""Tests for improved router: new formats, content sniffing, confidence boosting."""
from __future__ import annotations

import json
import struct
import zipfile
import io
import pytest

from document_processor.models import Document
from router_module.router import RouterModule

router = RouterModule()


def _doc(content: bytes = b"", filename: str = "", mimetype: str = "") -> Document:
    return Document(content=content, filename=filename, mimetype=mimetype)


# ---------------------------------------------------------------------------
# New MIME types
# ---------------------------------------------------------------------------

def test_mime_yaml():
    r = router._route(_doc(b"key: value\n", mimetype="application/yaml"))
    assert r[0] == "yaml"


def test_mime_x_yaml():
    r = router._route(_doc(b"key: value\n", mimetype="application/x-yaml"))
    assert r[0] == "yaml"


def test_mime_tsv():
    r = router._route(_doc(b"a\tb\tc\n1\t2\t3\n", mimetype="text/tab-separated-values"))
    assert r[0] == "spreadsheet"


def test_mime_ndjson():
    r = router._route(_doc(b'{"a":1}\n{"b":2}\n', mimetype="application/x-ndjson"))
    assert r[0] == "json"


def test_mime_heic():
    r = router._route(_doc(b"\x00\x00\x00\x0c", mimetype="image/heic"))
    assert r[0] == "image"


def test_mime_odt():
    r = router._route(_doc(b"", mimetype="application/vnd.oasis.opendocument.text"))
    assert r[0] == "word"


def test_mime_ods():
    r = router._route(_doc(b"", mimetype="application/vnd.oasis.opendocument.spreadsheet"))
    assert r[0] == "spreadsheet"


def test_mime_odp():
    r = router._route(_doc(b"", mimetype="application/vnd.oasis.opendocument.presentation"))
    assert r[0] == "presentation"


def test_mime_7zip():
    r = router._route(_doc(b"", mimetype="application/x-7z-compressed"))
    assert r[0] == "archive"


# ---------------------------------------------------------------------------
# New file extensions
# ---------------------------------------------------------------------------

def test_ext_yaml():
    r = router._route(_doc(b"", filename="config.yaml"))
    assert r[0] == "yaml"


def test_ext_yml():
    r = router._route(_doc(b"", filename="config.yml"))
    assert r[0] == "yaml"


def test_ext_tsv():
    r = router._route(_doc(b"", filename="data.tsv"))
    assert r[0] == "spreadsheet"


def test_ext_jsonl():
    r = router._route(_doc(b"", filename="records.jsonl"))
    assert r[0] == "json"


def test_ext_ndjson():
    r = router._route(_doc(b"", filename="records.ndjson"))
    assert r[0] == "json"


def test_ext_markdown():
    r = router._route(_doc(b"", filename="readme.markdown"))
    assert r[0] == "text"


def test_ext_rst():
    r = router._route(_doc(b"", filename="docs.rst"))
    assert r[0] == "text"


def test_ext_mbox():
    r = router._route(_doc(b"", filename="mailbox.mbox"))
    assert r[0] == "email"


def test_ext_heic():
    r = router._route(_doc(b"", filename="photo.heic"))
    assert r[0] == "image"


def test_ext_numbers():
    r = router._route(_doc(b"", filename="sheet.numbers"))
    assert r[0] == "spreadsheet"


# ---------------------------------------------------------------------------
# Magic bytes — new signatures
# ---------------------------------------------------------------------------

def test_magic_yaml_triple_dash():
    r = router._route(_doc(b"---\nfoo: bar\n"))
    assert r[0] == "yaml"


def test_magic_yaml_percent():
    r = router._route(_doc(b"%YAML 1.2\n---\nfoo: bar\n"))
    assert r[0] == "yaml"


def test_magic_gzip():
    r = router._route(_doc(b"\x1f\x8b\x08" + b"\x00" * 10))
    assert r[0] == "archive"


def test_magic_7zip():
    r = router._route(_doc(b"7z\xbc\xaf'\x1c" + b"\x00" * 10))
    assert r[0] == "archive"


def test_magic_rar():
    r = router._route(_doc(b"Rar!\x1a\x07" + b"\x00" * 10))
    assert r[0] == "archive"


def test_magic_email_received():
    r = router._route(_doc(b"Received: from mail.example.com\r\nFrom: user@example.com\r\n"))
    assert r[0] == "email"


def test_magic_email_message_id():
    r = router._route(_doc(b"Message-ID: <abc123@example.com>\r\nFrom: x@y.com\r\n"))
    assert r[0] == "email"


def test_magic_x_mailer():
    r = router._route(_doc(b"X-Mailer: Outlook\r\nFrom: x@y.com\r\n"))
    assert r[0] == "email"


# ---------------------------------------------------------------------------
# UTF-8 BOM stripping
# ---------------------------------------------------------------------------

def test_bom_html():
    # UTF-8 BOM + HTML should still route to html
    bom = b"\xef\xbb\xbf"
    r = router._route(_doc(bom + b"<!DOCTYPE html><html></html>"))
    assert r[0] == "html"


def test_bom_xml():
    bom = b"\xef\xbb\xbf"
    r = router._route(_doc(bom + b"<?xml version='1.0'?><root/>"))
    assert r[0] == "xml"


# ---------------------------------------------------------------------------
# NDJSON detection
# ---------------------------------------------------------------------------

def test_ndjson_content():
    content = b'{"id":1,"name":"Alice"}\n{"id":2,"name":"Bob"}\n'
    r = router._route(_doc(content, mimetype="application/json"))
    assert r[0] == "json"


# ---------------------------------------------------------------------------
# CSV/TSV content sniffing
# ---------------------------------------------------------------------------

def test_csv_sniff_plain_text_mime():
    csv_content = b"name,age,city\nAlice,30,London\nBob,25,Paris\nCarol,35,Berlin\n"
    r = router._route(_doc(csv_content, mimetype="text/plain"))
    assert r[0] == "spreadsheet"
    assert r[2] == "content_sniff"


def test_tsv_sniff_plain_text_mime():
    tsv_content = b"name\tage\tcity\nAlice\t30\tLondon\nBob\t25\tParis\nCarol\t35\tBerlin\n"
    r = router._route(_doc(tsv_content, mimetype="text/plain"))
    assert r[0] == "spreadsheet"
    assert r[2] == "content_sniff"


def test_csv_sniff_no_extension():
    csv_content = b"id,value,label\n1,100,foo\n2,200,bar\n3,300,baz\n"
    r = router._route(_doc(csv_content))
    assert r[0] == "spreadsheet"
    assert r[2] == "content_sniff"


def test_non_csv_text_not_sniffed():
    text = b"This is a normal paragraph.\nIt has no CSV structure whatsoever.\nJust words.\n"
    r = router._route(_doc(text, mimetype="text/plain"))
    # Should NOT be spreadsheet
    assert r[0] != "spreadsheet"


def test_too_few_lines_not_sniffed():
    # Only 2 rows — below minimum
    csv_content = b"name,age\nAlice,30\n"
    r = router._route(_doc(csv_content))
    assert r[0] != "spreadsheet"


# ---------------------------------------------------------------------------
# Multi-signal confidence boosting
# ---------------------------------------------------------------------------

def test_confidence_boost_magic_plus_mime():
    # PDF magic + declared PDF mime → confidence > 0.95
    pdf_content = b"%PDF-1.4 test"
    r = router._route(_doc(pdf_content, mimetype="application/pdf"))
    assert r[0] == "pdf"
    assert r[1] >= 0.95


def test_confidence_boost_magic_plus_ext():
    pdf_content = b"%PDF-1.4 test"
    r = router._route(_doc(pdf_content, filename="invoice.pdf"))
    assert r[0] == "pdf"
    assert r[1] >= 0.95


def test_confidence_mime_ext_agree():
    r = router._route(_doc(b"", filename="data.csv", mimetype="text/csv"))
    assert r[0] == "spreadsheet"
    assert r[1] >= 0.75


def test_confidence_magic_only():
    # JPEG magic but no other signals
    jpeg = b"\xff\xd8\xff\xe0" + b"\x00" * 20
    r = router._route(_doc(jpeg))
    assert r[0] == "image"
    assert r[1] == 0.95


# ---------------------------------------------------------------------------
# OLE2 mime fallback
# ---------------------------------------------------------------------------

def test_ole_with_msg_mime():
    ole_content = b"\xd0\xcf\x11\xe0" + b"\x00" * 512
    r = router._route(_doc(ole_content, mimetype="application/vnd.ms-outlook"))
    assert r[0] == "email"


# ---------------------------------------------------------------------------
# ZIP with ODF indicator
# ---------------------------------------------------------------------------

def test_zip_odf_content_xml():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("content.xml", "<office:document/>")
        zf.writestr("META-INF/manifest.xml", "<manifest/>")
    buf.seek(0)
    r = router._route(_doc(buf.read()))
    # content.xml before META-INF in ZIP_INDICATORS → should match word
    assert r[0] == "word"
