import sys

from loguru import logger

from document_processor.config import settings

logger.remove()
logger.add(
    sys.stderr,
    level=settings.log_level,
    format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | {message}",
)
logger.add(
    settings.log_file,
    level="DEBUG",
    serialize=True,
    rotation="100 MB",
    retention="30 days",
)
