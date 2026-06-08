FROM python:3.12-slim

WORKDIR /app

# Install system deps needed by optional OCR packages (tesseract) if added later
RUN apt-get update && apt-get install -y --no-install-recommends \
    tesseract-ocr \
    && rm -rf /var/lib/apt/lists/*

# Copy project definition first so pip cache layer is stable
COPY pyproject.toml ./

# Install all Python dependencies (no build isolation so editable install works)
COPY src/ src/
RUN pip install --no-cache-dir -e "." \
    && pip install --no-cache-dir uvicorn[standard]

# Persistent volumes
RUN mkdir -p /app/data /app/logs

# Non-root user
RUN adduser --disabled-password --gecos "" appuser \
    && chown -R appuser:appuser /app
USER appuser

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"

CMD ["uvicorn", "document_processor.api:app", \
     "--host", "0.0.0.0", "--port", "8000", \
     "--log-level", "info"]
