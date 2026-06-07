from typing import Protocol, runtime_checkable

from document_processor.models import Document, StageResult


@runtime_checkable
class Module(Protocol):
    """
    Contract every module must satisfy.

    Each module repo must ship a class that implements this protocol.
    The orchestrator discovers modules via the registry and calls them
    in pipeline order, passing the accumulated context so later stages
    can read earlier stages' output.
    """

    name: str

    async def process(self, document: Document, context: dict) -> StageResult:
        """
        Process the document and return a StageResult.

        context is a dict keyed by module name containing each previous
        stage's StageResult.data, so a module can read upstream output.
        """
        ...

    async def health_check(self) -> bool:
        """Return True if the module is ready to process documents."""
        ...
