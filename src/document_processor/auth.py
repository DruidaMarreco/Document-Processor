"""Optional API key authentication.

When API keys exist in the database, every request to protected endpoints must
carry `Authorization: Bearer <key>`.  If *no* keys have been registered the
check is a no-op — the system starts open and locks down once a first key is
created.
"""
from __future__ import annotations

from fastapi import HTTPException, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from document_processor import storage

_bearer = HTTPBearer(auto_error=False)


async def require_api_key(
    credentials: HTTPAuthorizationCredentials | None = Security(_bearer),
) -> None:
    keys = await storage.list_api_keys()
    if not keys:
        return  # no keys registered → open access

    if not credentials or credentials.scheme.lower() != "bearer":
        raise HTTPException(status_code=401, detail="API key required")

    if not await storage.verify_api_key(credentials.credentials):
        raise HTTPException(status_code=403, detail="Invalid API key")
