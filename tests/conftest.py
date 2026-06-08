"""Global pytest fixtures shared across all test modules."""
from __future__ import annotations

import pytest

from document_processor.api import limiter


@pytest.fixture(autouse=True)
def reset_rate_limiter():
    """Clear slowapi in-memory counters before every test so rate limits don't bleed across tests."""
    limiter._storage.reset()
    yield
