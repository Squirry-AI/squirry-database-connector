# register_db.py

import logging
import yaml
import os
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import make_url
from pathlib import Path
from dotenv import load_dotenv
from .helpers import get_describe_table_statement, get_list_tables_statement, get_password_environment_variable, infer_kind_from_url, infer_port, normalize_url
from .constants import MYSQL, POSTGRES, ParameterTypes

load_dotenv()
logger = logging.getLogger(__name__)

def register_database(tools_yaml_path: str, db_key: str, connection_url: str) -> list[str]:
    connection_url = normalize_url(connection_url)
    engine = create_engine(connection_url)
    inspector = inspect(engine)
    tables = inspector.get_table_names()

    parsed = make_url(connection_url)
    kind = infer_kind_from_url(connection_url)
    port = parsed.port or infer_port(kind)

    tools_yaml = Path(tools_yaml_path)
    config = yaml.safe_load(tools_yaml.read_text()) if tools_yaml.exists() else {}
    
    # Build source config
    source_config = {
        "kind": kind,
        "database": parsed.database,
    }
    
    if kind in [POSTGRES, MYSQL]:
        source_config["port"] = port
        source_config["host"] = parsed.host
        source_config["user"] = parsed.username
        source_config["password"] = f"{get_password_environment_variable(kind)}"
    
    config.setdefault("sources", {})[db_key] = source_config

    cfg_tools = config.setdefault("tools", {})
    cfg_tools[f"{db_key}_list_tables"] = {
        "kind": f"{kind}-sql",
        "source": db_key,
        "description": f"List tables in {db_key}",
        "statement": get_list_tables_statement(kind)
    }

    cfg_tools[f"{db_key}_describe_table"] = {
        "kind": f"{kind}-sql",
        "source": db_key,
        "description": f"Describe columns in a table of {db_key}",
        ParameterTypes[kind]: [
            {
                "name": "table",
                "type": "string",
                "description": "Name of table to inspect"
            }
        ],
        "statement": get_describe_table_statement(kind)
    }

    if kind in [POSTGRES, MYSQL]:
        cfg_tools[f"{db_key}_execute_query"] = {
            "kind": f"{kind}-execute-sql",
            "source": db_key,
            "description": f"Execute arbitrary SQL on {db_key} within the 'sql' parameter"
        }
    else:
        cfg_tools[f"{db_key}_execute_query"] = {
            "kind": f"{kind}-sql",
            "source": db_key,
            "description": f"Execute arbitrary SQL on {db_key} within the 'sql' parameter",
            ParameterTypes[kind]: [
                {
                    "name": "sql",
                    "type": "string",
                    "description": "SQL query to execute"
                }
            ],
            "statement": "{{.sql}};"
        }
        

    toolsets = config.setdefault("toolsets", {})
    toolsets[f"{db_key}_toolset"] = [
        f"{db_key}_list_tables",
        f"{db_key}_describe_table",
        f"{db_key}_execute_query"
    ]

    tools_yaml.write_text(yaml.safe_dump(config))
    logger.info(f"Registered '{db_key}' successfully; found tables: {tables}")  # Keep print for CLI use
    return tables

if __name__ == "__main__":
    # Register PostgreSQL database
    # register_database(
    #     os.getenv("TOOLS_YAML_PATH"),
    #     os.getenv("DB_KEY_POSTGRES"),
    #     os.getenv("CONNECTION_URL_POSTGRES")
    # )

    # Register MySQL database
    # register_database(
    #     os.getenv("TOOLS_YAML_PATH"),
    #     os.getenv("DB_KEY_MYSQL"),
    #     os.getenv("CONNECTION_URL_MYSQL")
    # )

    # Register SQLite database
    register_database(
        os.getenv("TOOLS_YAML_PATH"),
        os.getenv("DB_KEY_SQLITE"),
        os.getenv("CONNECTION_URL_SQLITE")
    )