# utils/db/helpers.py

from .constants import (
    POSTGRES,
    MYSQL,
    POSTGRES_URL_PREFIX,
    MYSQL_URL_PREFIX,
    LEGACY_POSTGRES_URL_PREFIX,
    SQLITE,
)

def infer_kind_from_url(url: str) -> str:
    """Infers the database kind from a connection URL."""
    if url.startswith(POSTGRES_URL_PREFIX):
        return POSTGRES
    if url.startswith(MYSQL_URL_PREFIX) or url.startswith("mysql+pymysql://"):
        return MYSQL
    if url.startswith("file:") or url.startswith("sqlite://"):
        return "sqlite"
    raise ValueError(f"Unsupported DB dialect in URL: {url}")

def infer_port(kind: str, default_port: int = None) -> int:
    """Infers the port for a given database kind."""
    if kind == POSTGRES:
        return 5432 if default_port is None else default_port
    if kind == MYSQL:
        return 3306 if default_port is None else default_port
    if kind == SQLITE:
        return None
    raise ValueError(f"Unsupported DB kind: {kind}")

def get_password_environment_variable(kind: str) -> str:
    """Returns the environment variable name for the database password."""
    if kind == POSTGRES:
        return "${DB_PASSWORD_POSTGRES}"
    if kind == MYSQL:
        return "${DB_PASSWORD_MYSQL}"
    if kind == SQLITE:
        return None
    raise ValueError(f"Unsupported DB kind for password: {kind}")

def normalize_url(url: str) -> str:
    """Normalizes a database connection URL."""
    if url.startswith(LEGACY_POSTGRES_URL_PREFIX):
        return url.replace(LEGACY_POSTGRES_URL_PREFIX, POSTGRES_URL_PREFIX, 1)
    
    # Convert mysql:// to mysql+pymysql:// to use PyMySQL driver
    if url.startswith(MYSQL_URL_PREFIX):
        return url.replace(MYSQL_URL_PREFIX, "mysql+pymysql://", 1)
    return url

def get_describe_table_statement(kind: str) -> str:
    """Returns the SQL statement to describe a table based on the database kind."""
    if kind == POSTGRES:
        return (
            "SELECT column_name, data_type "
            "FROM information_schema.columns "
            "WHERE table_name = $1;"
        )
    if kind == MYSQL:
        return (
            "SELECT column_name, data_type "
            "FROM information_schema.columns "
            "WHERE table_name = ?;"
        )
    if kind == SQLITE:
        return (
            "PRAGMA table_info({{.table}});"
        )
    raise ValueError(f"Unsupported DB kind for describe table: {kind}")

def get_list_tables_statement(kind: str) -> str:
    """Returns the SQL statement to list tables based on the database kind."""
    if kind == POSTGRES:
        return (
            "SELECT table_name "
            "FROM information_schema.tables "
            "WHERE table_schema='public';"
        )
    if kind == MYSQL:
        return (
            "SHOW TABLES;"
        )
    if kind == SQLITE:
        return (
            "SELECT name FROM sqlite_master "
            "WHERE type='table';"
        )
    raise ValueError(f"Unsupported DB kind for list tables: {kind}")