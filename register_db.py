# register_db.py

import yaml
import os
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import make_url
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

def infer_kind_from_url(url):
    if url.startswith("postgresql://"):
        return "postgres"
    if url.startswith("mysql://"):
        return "mysql"
    raise ValueError(f"Unsupported DB dialect in URL: {url}")

def normalize_url(url: str) -> str:
    if url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql://", 1)
    return url

def register_database(tools_yaml_path: str, db_key: str, connection_url: str):
    connection_url = normalize_url(connection_url)
    engine = create_engine(connection_url)
    inspector = inspect(engine)
    tables = inspector.get_table_names()

    parsed = make_url(connection_url)
    kind = infer_kind_from_url(connection_url)

    tools_yaml = Path(tools_yaml_path)
    config = yaml.safe_load(tools_yaml.read_text()) if tools_yaml.exists() else {}
    config.setdefault("sources", {})[db_key] = {
        "kind": kind,
        "host": parsed.host,
        "port": parsed.port or (5432 if kind == "postgres" else None),
        "database": parsed.database,
        "user": parsed.username,
        "password": "${DB_PASSWORD}",
    }

    cfg_tools = config.setdefault("tools", {})
    cfg_tools[f"{db_key}_list_tables"] = {
        "kind": f"{kind}-sql",
        "source": db_key,
        "description": f"List tables in {db_key}",
        "statement": (
            "SELECT table_name "
            "FROM information_schema.tables "
            "WHERE table_schema='public';"
        )
    }

    cfg_tools[f"{db_key}_describe_table"] = {
        "kind": f"{kind}-sql",
        "source": db_key,
        "description": f"Describe columns in a table of {db_key}",
        "parameters": [
            {
                "name": "table",
                "type": "string",
                "description": "Name of table to inspect"
            }
        ],
        "statement": (
            "SELECT column_name, data_type "
            "FROM information_schema.columns "
            "WHERE table_name = $1;"
        )
    }

    cfg_tools[f"{db_key}_execute_query"] = {
        "kind": f"{kind}-sql",
        "source": db_key,
        "description": f"Execute arbitrary SQL on {db_key}",
        "parameters": [
            {
                "name": "query",
                "type": "string",
                "description": "SQL query string to run"
            }
        ],
        "statement": "$1"
    }

    toolsets = config.setdefault("toolsets", {})
    toolsets[f"{db_key}_toolset"] = [
        f"{db_key}_list_tables",
        f"{db_key}_describe_table",
        f"{db_key}_execute_query"
    ]

    tools_yaml.write_text(yaml.safe_dump(config))
    print(f"Registered '{db_key}' successfully; found tables: {tables}")

if __name__ == "__main__":
    register_database(
        os.getenv("TOOLS_YAML_PATH"),
        os.getenv("DB_KEY"),
        os.getenv("CONNECTION_URL")
    )
