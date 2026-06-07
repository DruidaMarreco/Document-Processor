from __future__ import annotations

from pathlib import Path

# (doc_type, keywords) — higher-weight keywords should be specific phrases
TYPE_KEYWORDS: list[tuple[str, list[str]]] = [
    ("invoice",       ["invoice", "bill to", "invoice no", "invoice #", "amount due", "due date", "remit to", "payment terms"]),
    ("receipt",       ["receipt", "thank you for your purchase", "total paid", "payment received", "cash receipt"]),
    ("purchase_order",["purchase order", "po number", "po #", "ship to", "vendor", "order date", "ordered by"]),
    ("contract",      ["contract", "agreement", "parties agree", "whereas", "hereinafter", "terms and conditions", "in witness whereof"]),
    ("statement",     ["account statement", "statement of account", "opening balance", "closing balance", "transactions"]),
    ("form",          ["please complete", "fill in", "applicant", "application form", "questionnaire", "check all that apply"]),
    ("report",        ["executive summary", "prepared by", "findings", "conclusion", "recommendations", "methodology"]),
    ("resume",        ["curriculum vitae", " cv ", "work experience", "employment history", "education", "references available"]),
    ("letter",        ["dear ", "sincerely,", "yours truly", "to whom it may concern", "kind regards", "best regards"]),
    ("specification", ["specification", "requirements document", "revision history", "scope of work", "deliverables"]),
]

ROUTE_TO_TYPE: dict[str, str] = {
    "image":        "image",
    "spreadsheet":  "spreadsheet",
    "presentation": "presentation",
    "email":        "email",
    "html":         "webpage",
    "json":         "data",
    "xml":          "data",
    "archive":      "archive",
}
