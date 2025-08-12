# logger.py
import logging
from rich.logging import RichHandler

def setup_logging(level: str = "INFO"):
    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(rich_tracebacks=True)]
    )

# Call setup at import or main entry
setup_logging()

# Create and export logger instance
logger = logging.getLogger(__name__)
