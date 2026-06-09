from __future__ import annotations

from pathlib import Path

# Each entry: (doc_type, high_weight_keywords, low_weight_keywords)
# High-weight = distinctive phrases unique to this type (weight 3)
# Low-weight  = supporting terms that may appear elsewhere (weight 1)
TYPE_KEYWORDS_WEIGHTED: list[tuple[str, list[str], list[str]]] = [
    ("invoice", [
        "invoice", "tax invoice", "vat invoice", "invoice number", "invoice no",
        "invoice #", "bill to", "remit to", "amount due", "net 30", "net 60",
        "payment terms",
    ], [
        "subtotal", "due date", "purchase", "payment",
    ]),
    ("receipt", [
        "receipt", "receipt number", "cash receipt", "thank you for your purchase",
        "total paid", "payment received", "amount paid", "change due",
    ], [
        "transaction id", "purchase", "total",
    ]),
    ("purchase_order", [
        "purchase order", "po number", "po #", "po no", "purchase order number",
    ], [
        "ship to", "vendor", "order date", "ordered by", "unit price", "qty", "line total",
    ]),
    ("contract", [
        "in witness whereof", "hereinafter referred to", "parties agree",
        "whereas", "indemnification", "governing law", "executed by",
        "termination clause",
    ], [
        "contract", "agreement", "obligations", "terms and conditions",
        "effective date", "hereinafter",
    ]),
    ("nda", [
        "non-disclosure agreement", "non-disclosure", "nda",
        "confidentiality agreement", "disclosing party", "receiving party",
        "shall not disclose", "trade secret",
    ], [
        "confidential information", "proprietary information", "disclosure",
    ]),
    ("statement", [
        "account statement", "statement of account", "opening balance",
        "closing balance", "balance forward",
    ], [
        "debit", "credit", "transactions",
    ]),
    ("bank_statement", [
        "bank statement", "account holder", "sort code", "swift", "bic",
        "available balance", "overdraft", "iban",
    ], [
        "account number", "branch", "debit", "credit", "transactions",
    ]),
    ("tax_document", [
        "tax return", "w-2", "w2", "1099", "irs", "adjusted gross income",
        "employer identification", "filing status", "taxable income",
        "social security number",
    ], [
        "gross income", "federal tax", "state tax", "tax year", "withholding",
    ]),
    ("payslip", [
        "payslip", "pay stub", "pay slip", "earnings statement",
        "gross pay", "net pay", "national insurance", "year to date", "ytd",
    ], [
        "deductions", "employer", "employee", "pay period", "hours worked",
        "pension", "withholding",
    ]),
    ("insurance", [
        "policy number", "insurance certificate", "policyholder",
        "underwriter", "deductible", "insured",
    ], [
        "insurance", "premium", "coverage", "claim", "beneficiary",
        "effective coverage", "expiry date",
    ]),
    ("medical_record", [
        "patient", "diagnosis", "prescription", "physician", "icd",
        "cpt", "medical history", "date of birth", "referral",
    ], [
        "doctor", "clinic", "hospital", "symptoms", "treatment",
        "medication", "dosage",
    ]),
    ("quote", [
        "quotation", "quote #", "quote number", "valid until", "quoted by",
        "total estimate", "validity period",
    ], [
        "estimate", "proposal", "unit cost", "discount",
        "quote date", "labour cost", "parts cost",
    ]),
    ("delivery_note", [
        "delivery note", "packing list", "packing slip", "despatch note",
        "dispatch note", "consignment", "goods received",
    ], [
        "tracking number", "shipped to", "carrier", "delivery date",
    ]),
    ("form", [
        "application form", "please complete", "fill in all fields",
        "check all that apply", "please print clearly",
    ], [
        "applicant", "questionnaire", "signature", "date of birth",
    ]),
    ("report", [
        "executive summary", "table of contents", "findings",
        "recommendations", "methodology", "data analysis",
    ], [
        "prepared by", "conclusion", "appendix",
    ]),
    ("resume", [
        "curriculum vitae", "work experience", "employment history",
        "references available", "professional summary",
    ], [
        " cv ", "education", "skills", "objective",
        "certifications", "linkedin",
    ]),
    ("letter", [
        "to whom it may concern", "dear sir or madam", "sincerely,",
        "yours truly", "yours faithfully", "kind regards", "best regards",
    ], [
        "dear ", "re:", "subject:",
    ]),
    ("specification", [
        "requirements document", "functional requirements", "non-functional requirements",
        "acceptance criteria", "scope of work", "deliverables",
    ], [
        "specification", "revision history", "user story",
    ]),
    ("email", [
        "from:", "to:", "subject:", "sent:", "cc:", "bcc:",
        "reply-to:", "message-id:",
    ], [
        "forwarded message", "original message", "wrote:",
    ]),
]

# Flat keyword list kept for backward compatibility (e.g. filename matching)
TYPE_KEYWORDS: list[tuple[str, list[str]]] = [
    (doc_type, high + low)
    for doc_type, high, low in TYPE_KEYWORDS_WEIGHTED
]

ROUTE_TO_TYPE: dict[str, str] = {
    "image":        "image",
    "spreadsheet":  "spreadsheet",
    "presentation": "presentation",
    "email":        "email",
    "html":         "webpage",
    "json":         "data",
    "xml":          "data",
    "yaml":         "data",
    "archive":      "archive",
}

# Stem patterns that strongly suggest a doc type when the filename matches
FILENAME_PATTERNS: list[tuple[str, list[str]]] = [
    ("invoice",        ["invoice", "inv_", "inv-", "faktura"]),
    ("receipt",        ["receipt", "rcpt"]),
    ("purchase_order", ["purchase_order", "po_", "po-"]),
    ("contract",       ["contract", "agreement", "agmt"]),
    ("nda",            ["nda", "nondisclosure", "non_disclosure"]),
    ("bank_statement", ["bank_statement", "bankstatement", "account_statement"]),
    ("tax_document",   ["tax_return", "w2", "w-2", "1099"]),
    ("payslip",        ["payslip", "pay_stub", "paystub", "paycheck"]),
    ("quote",          ["quote", "quotation", "estimate_"]),
    ("delivery_note",  ["delivery_note", "packing_list", "dispatch_note"]),
    ("resume",         ["resume", "cv_", "_cv", "curriculum_vitae"]),
    ("report",         ["report_", "_report"]),
]

# Weights
_HIGH_WEIGHT = 3
_LOW_WEIGHT = 1

# Precomputed max possible score per type (used for confidence normalization)
_MAX_SCORE: dict[str, int] = {
    doc_type: len(high) * _HIGH_WEIGHT + len(low) * _LOW_WEIGHT
    for doc_type, high, low in TYPE_KEYWORDS_WEIGHTED
}
