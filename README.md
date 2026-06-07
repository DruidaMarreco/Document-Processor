# Document Processor

Orchestrator for a universal document processing pipeline. Accepts any document type and routes it through a chain of specialised modules, each living in its own repository.

## Architecture

```
                         ┌─────────────────────────────┐
  HTTP POST /process ───▶│     Document Processor       │
                         │        (orchestrator)         │
                         └──────────┬──────────────────-┘
                                    │ in-process calls
                    ┌───────────────▼──────────────────────┐
                    │                                        │
              ┌─────▼──────┐                                │
              │   Router   │  picks the right pipeline      │
              └─────┬──────┘                                │
              ┌─────▼──────┐                                │
              │ Classifier │  labels document type          │
              └─────┬──────┘                                │
              ┌─────▼──────┐                                │
              │  Extractor │  pulls structured fields       │
              └─────┬──────┘                                │
              ┌─────▼──────┐                                │
              │  Refiner   │  normalises & cleans           │
              └─────┬──────┘                                │
              ┌─────▼──────┐                                │
              │ Validator  │  checks rules / schemas        │
              └─────┬──────┘                                │
              ┌─────▼──────┐                                │
              │ Generator  │  produces final output         │
              └─────┬──────┘                                │
                    └───────────────────────────────────────┘
                                    │
                         ┌──────────▼──────────┐
                         │  Monitoring Module  │  aggregates logs / metrics
                         └─────────────────────┘
```

### Module Repositories

| Repo | Package | Role |
|---|---|---|
| [Router-Module](https://github.com/DruidaMarreco/Router-Module) | `router_module` | Detects document type and selects pipeline |
| [Classifier-Module](https://github.com/DruidaMarreco/Classifier-Module) | `classifier_module` | Classifies document (invoice, contract, form…) |
| [Extractor-Module](https://github.com/DruidaMarreco/Extractor-Module) | `extractor_module` | Extracts structured fields from raw content |
| [Refiner-Module](https://github.com/DruidaMarreco/Refiner-Module) | `refiner_module` | Normalises and cleans extracted data |
| [Validator-Module](https://github.com/DruidaMarreco/Validator-Module) | `validator_module` | Validates data against schemas and rules |
| [Generator-Module](https://github.com/DruidaMarreco/Generator-Module) | `generator_module` | Generates the final structured output |
| [Monitoring-Module](https://github.com/DruidaMarreco/Monitoring-Module) | `monitoring_module` | Dashboard and log aggregation |

### Module Contract

Every module must implement the `Module` protocol defined in `src/document_processor/modules/base.py`:

```python
class MyModule:
    name = "my_module"

    async def process(self, document: Document, context: dict) -> StageResult:
        # context contains every previous stage's StageResult.data keyed by module name
        ...

    async def health_check(self) -> bool:
        ...
```

### Communication

Modules are **Python packages imported directly** — no network hop, no broker.  
Each stage receives the full `Document` object plus a `context` dict containing all previous stages' output data.  
All stages emit structured JSON logs (via `loguru`) that the Monitoring Module will aggregate.

### Replacing a Stub

```python
# registry.py — before
from document_processor.modules.stubs import ClassifierModuleStub
stages.append(ClassifierModuleStub())

# registry.py — after (once classifier-module package is installed)
from classifier_module import ClassifierModule
stages.append(ClassifierModule())
```

## Setup

```bash
pip install -e ".[dev]"
```

## Run

```bash
python -m document_processor
# or
uvicorn document_processor.api:app --reload
```

API available at `http://localhost:8000`. Docs at `http://localhost:8000/docs`.

## Test

```bash
pytest
```

## API

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Health check |
| `POST` | `/process` | Upload a document (multipart) |
| `GET` | `/results/{id}` | Get a processing result |
| `GET` | `/results` | List all results (in-memory) |

Logs are written to `logs/document_processor.jsonl` in structured JSON format.
