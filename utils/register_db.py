# register_db.py

import yaml
import os
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import make_url
from pathlib import Path
from dotenv import load_dotenv
from helpers import get_describe_table_statement, get_list_tables_statement, get_password_environment_variable, infer_kind_from_url, infer_port, normalize_url
from constants import POSTGRES

load_dotenv()

def register_database(tools_yaml_path: str, db_key: str, connection_url: str):
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
        "host": parsed.host,
        "port": port,
        "database": parsed.database,
        "user": parsed.username,
    }
    
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
        "parameters": [
            {
                "name": "table",
                "type": "string",
                "description": "Name of table to inspect"
            }
        ],
        "statement": get_describe_table_statement(kind)
    }

    cfg_tools[f"{db_key}_execute_query"] = {
        "kind": f"{kind}-execute-sql",
        "source": db_key,
        "description": f"Execute arbitrary SQL on {db_key} within the 'sql' parameter"
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
    # Register PostgreSQL database
    register_database(
        os.getenv("TOOLS_YAML_PATH"),
        os.getenv("DB_KEY_POSTGRES"),
        os.getenv("CONNECTION_URL_POSTGRES")
    )

    # Register MySQL database
    # register_database(
    #     os.getenv("TOOLS_YAML_PATH"),
    #     os.getenv("DB_KEY_MYSQL"),
    #     os.getenv("CONNECTION_URL_MYSQL")
    # )
