import uvicorn

from document_processor.config import settings

if __name__ == "__main__":
    uvicorn.run(
        "document_processor.api:app",
        host=settings.host,
        port=settings.port,
        reload=True,
    )
