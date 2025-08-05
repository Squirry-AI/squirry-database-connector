# utils/db/helpers.py

from constants import (
    POSTGRES,
    MYSQL,
    POSTGRES_URL_PREFIX,
    MYSQL_URL_PREFIX,
    LEGACY_POSTGRES_URL_PREFIX,
)

def infer_kind_from_url(url: str) -> str:
    """Infers the database kind from a connection URL."""
    if url.startswith(POSTGRES_URL_PREFIX):
        return POSTGRES
    if url.startswith(MYSQL_URL_PREFIX):
        return MYSQL
    raise ValueError(f"Unsupported DB dialect in URL: {url}")

def normalize_url(url: str) -> str:
    """Normalizes a database connection URL."""
    if url.startswith(LEGACY_POSTGRES_URL_PREFIX):
        return url.replace(LEGACY_POSTGRES_URL_PREFIX, POSTGRES_URL_PREFIX, 1)
    return url
