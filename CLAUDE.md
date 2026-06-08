# Document Processor — Codebase Map

Universal document processing pipeline: PDFs, invoices, contracts, emails, images, spreadsheets, and more. Every document flows through a fixed 6-stage async pipeline; a cross-cutting monitoring module records every stage result for a live SSE dashboard.

## Architecture

```
POST /process (or /process/stream, /batch)
        │
        ▼
  ┌─────────────────────────────────────────────────────────┐
  │                     Pipeline                            │
  │  Router → Classifier → Extractor → Refiner →           │
  │  Validator → Generator                                  │
  │                    │ (listener after every stage)       │
  │              MonitoringModule                           │
  └─────────────────────────────────────────────────────────┘
        │
        ▼
  SQLite (data/results.db)   +   SSE Dashboard (/monitoring)
```

All module communication is **in-process Python** — no message queue, no network hop. Each stage receives `(document: Document, context: dict)` and returns a `StageResult`; the pipeline accumulates stage outputs in `context[module.name]` so later stages can read earlier ones.

## Repository layout (monorepo)

```
src/
  document_processor/     # orchestrator + FastAPI app
    api.py               # HTTP endpoints (process, batch, stream, results)
    pipeline.py          # async Pipeline class + listener pattern
    registry.py          # wires all modules together → build_pipeline()
    models.py            # Document, StageResult, PipelineResult (Pydantic v2)
    config.py            # Settings via pydantic-settings (env prefix DOC_PROCESSOR_)
    storage.py           # async SQLite persistence (aiosqlite)
    upload.html          # drag-and-drop single-page upload UI (served at /)
  router_module/         # magic-byte + MIME + extension routing
  classifier_module/     # keyword scoring + LLM fallback classification
  extractor_module/      # regex + pdfplumber/OCR + LLM field extraction
  refiner_module/        # date normalisation, amount parsing, deduplication
  validator_module/      # per-type required fields, email/date/amount rules
  generator_module/      # composes final JSON output + human summary
  monitoring_module/     # SSE event store, live dashboard, Prometheus metrics
  llm_client/            # AsyncAnthropic wrapper (tool-use, vision, graceful fallback)
tests/                   # pytest-asyncio test suite (93 tests)
```

## Pipeline stages

| Stage | Module | What it does |
|---|---|---|
| **Router** | `router_module` | Detects file type via magic bytes, MIME, extension → `route` string (`pdf`, `image`, `spreadsheet`, `email`, `html`, `json`, `word`, `generic`, …) |
| **Classifier** | `classifier_module` | Keyword scoring over document text → `doc_type` + `confidence`. Falls back to Claude (`claude-haiku-4-5`) when confidence < 0.50. Supports vision for image route. |
| **Extractor** | `extractor_module` | Route-aware extraction: pdfplumber for PDFs, pytesseract for images, DictReader for CSV, tag-stripping for HTML, regex for dates/amounts/emails/URLs. Claude enriches fields for complex types (invoice, contract, …). |
| **Refiner** | `refiner_module` | Normalises dates → ISO 8601, strips currency symbols → float, deduplicates list fields, passes through unrecognised keys. |
| **Validator** | `validator_module` | Checks per-type required fields (invoice needs amounts+dates; email needs emails; …), validates email format, detects non-ISO dates and negative amounts. Always returns `status="success"` — violations go into `data.violations`. |
| **Generator** | `generator_module` | Composes all stage outputs into a single structured JSON blob + human-readable summary string. |

## LLM integration

`src/llm_client/client.py` wraps `AsyncAnthropic` with two tool-use calls:

- `classify()` — forces `set_document_type` tool, returns `(doc_type, confidence)`
- `extract()` — forces `set_extracted_fields` tool, returns dict of typed field lists

Client is a lazy singleton (`_get_llm()`). Absent `ANTHROPIC_API_KEY` → `client.available = False` → graceful no-op fallback in both Classifier and Extractor. All SDK errors are caught and logged; the heuristic result is kept.

LLM config (all prefixed `DOC_PROCESSOR_`):

| Env var | Default | Meaning |
|---|---|---|
| `LLM_ENABLED` | `true` | Master switch |
| `LLM_MODEL` | `claude-haiku-4-5` | Anthropic model ID |
| `LLM_CONFIDENCE_THRESHOLD` | `0.50` | Trigger LLM in Classifier below this |
| `LLM_MAX_INPUT_CHARS` | `4000` | Chars sent to LLM |
| `LLM_VISION_ENABLED` | `true` | Send image bytes to LLM for image route |

## Key files

| File | Purpose |
|---|---|
| `src/document_processor/models.py` | Shared Pydantic models — start here |
| `src/document_processor/pipeline.py` | Core async loop; add `Listener` hooks here |
| `src/document_processor/registry.py` | Change which modules run or their order |
| `src/document_processor/config.py` | All tuneable settings |
| `src/document_processor/storage.py` | Swap SQLite for another store here |
| `src/monitoring_module/store.py` | `EventStore` — stats, SSE fanout, ring buffer |
| `src/llm_client/client.py` | All Anthropic API calls live here |

## HTTP endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/` | Upload UI (upload.html) |
| `POST` | `/process` | Process one file, return `PipelineResult` |
| `POST` | `/process/stream` | Process one file, stream SSE stage events + final result |
| `POST` | `/batch` | Process up to 20 files concurrently |
| `GET` | `/results` | List stored results (SQLite, `?limit=N`) |
| `GET` | `/results/{id}` | Fetch one result by document UUID |
| `GET` | `/health` | Health check + stored result count |
| `GET` | `/monitoring/` | Live SSE dashboard |
| `GET` | `/monitoring/events` | Raw SSE stream |
| `GET` | `/monitoring/stats` | Aggregate stats JSON |
| `GET` | `/monitoring/history` | Recent events JSON (`?n=100`) |
| `GET` | `/monitoring/metrics` | Prometheus text metrics |

## Development

```bash
pip install -e ".[dev]"          # install with dev extras
pytest                            # run all tests (93, ~2 s)
pytest tests/test_classifier.py  # run one file
uvicorn document_processor.api:app --reload  # dev server at :8000
```

Optional OCR (needs system `tesseract`):
```bash
pip install -e ".[ocr]"
```

## Testing

`tests/` uses `pytest-asyncio` in `auto` mode. All external calls (Anthropic SDK, pdfplumber, pytesseract) are mocked with `unittest.mock.AsyncMock` / `MagicMock` + `patch`. FastAPI endpoints are tested via `httpx.AsyncClient(ASGITransport(...))` — no live server needed.

Patch paths follow the import location, not the definition:
- `classifier_module.classifier.settings` (not `document_processor.config.settings`)
- `extractor_module.extractor._get_llm`

## Adding a new module

1. Create `src/new_module/` with `__init__.py` (exports `NewModule`) and `new_module.py`
2. Implement `async def process(self, document: Document, context: dict) -> StageResult`
3. Add to `packages` list in `pyproject.toml`
4. Import and insert into `stages` list in `registry.py`
5. Write tests in `tests/test_new_module.py`

## Deployment

```bash
docker compose up --build        # build + run
docker compose up -d             # detached
```

Data persists in `./data/results.db` and `./logs/` via volume mounts.
Set `ANTHROPIC_API_KEY` in your shell or a `.env` file — compose picks it up automatically.
