# Squirry DB Connector Agent
<img width="2240" height="1260" alt="Black and White Modern Minimalist Planter YouTube Banner (2)" src="https://github.com/user-attachments/assets/1849b02f-25c5-4350-9cac-f4536162954c" />

## Introduction
This project provides a dynamic agent capable of connecting to multiple databases (PostgreSQL, MySQL, SQLite) and converting natural language text into SQL queries. It uses a configuration-driven approach to dynamically select and connect to the desired database.

## Architecture Diagram
<img width="794" height="352" alt="Untitled design" src="https://github.com/user-attachments/assets/18d9f4b4-204d-4634-ab41-829be736fee1" />

## Features

-   **Dynamic Database Connectivity:** Connect to different SQL databases on the fly based on configuration.
-   **Multi-DB Support:** Out-of-the-box support for PostgreSQL, MySQL, and SQLite.
-   **Text-to-SQL:** Leverages Google's Large Language Models to translate natural language questions into executable SQL queries.
-   **Configuration-driven:** Easily configure database connections and tools via YAML and environment variables.

## Prerequisites

-   Python 3.8+
-   An active Google API Key with the necessary AI Platform APIs enabled.
-   Google's MCP Toolbox

## MCP Toolbox for Databases – Quickstart Guide

This guide walks you through installing and running the **MCP Toolbox for Databases** (an MCP server) on **macOS** and **Windows**, so you can connect your AI agents to enterprise data.



##  Prerequisites

- A supported database (e.g., PostgreSQL, MYSQL, SQLite).  
- `tools.yaml` configuration file defining your data sources and tools (This is generated on the fly within the app)  
- `curl` on macOS / Windows (via PowerShell) or Homebrew on macOS.  
:contentReference[oaicite:0]{index=0}

---

###  Step 1: Download the MCP Toolbox Binary

#### MacOS & Linux:
```bash
export VERSION=0.11.0
export OS="darwin/amd64"  # or "darwin/arm64"
curl -O https://storage.googleapis.com/genai-toolbox/v$VERSION/$OS/toolbox
chmod +x toolbox
```
OR
```bash
brew install mcp-toolbox
```

#### Windows
```bash
$env:VERSION="0.11.0"
$env:OS="windows/amd64"
Invoke-WebRequest "https://storage.googleapis.com/genai-toolbox/v$env:VERSION/$env:OS/toolbox.exe" -OutFile "toolbox.exe"
```

#### Here is an example of `tools.yaml` file
```yaml
sources:
  my-pg-source:
    kind: postgres
    host: 127.0.0.1
    port: 5432
    database: toolbox_db
    user: toolbox_user
    password: your-password

tools:
  search-hotels-by-location:
    kind: postgres-sql
    source: my-pg-source
    description: Finds hotels in a specific city.
    parameters:
      - name: location
        type: string
        description: City to search for hotels
    statement: SELECT * FROM hotels WHERE location = $1;
```

#### Run on MacOS & Linux:
```bash
./toolbox --tools-file "tools.yaml"
```

#### If installed via Homebrew on macOS/Linux:
```bash
toolbox --tools-file "tools.yaml"
```

### Run on Windows (PowerShell or CMD):
```bash
.\toolbox.exe --tools-file "tools.yaml"
```

---

### Installation

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/Squirry-AI/squirry-database-connector
    cd squirry-database-connector
    ```

2.  **Set up a virtual environment:**
    It is highly recommended to create and activate a virtual environment.
    ```bash
    python3 -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate`
    ```

3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
    *(Note: A `requirements.txt` file should be present in the repository. If not, you will need to install the necessary packages like `google-generativeai`, `psycopg2-binary`, `mysql-connector-python`, `PyYAML`, `python-dotenv` etc.)*

## Configuration

1.  **Environment Variables:**
    Create a `.env` file by copying the example file:
    ```bash
    cp .env.example .env
    ```
    Update the `.env` file with your specific configuration:

    -   `GOOGLE_API_KEY`: **(Required)** Your Google API Key.
    -   `TOOLS_YAML_PATH`: Path to your tools configuration file (default: `./config.yaml`).
    -   `DB_KEY`: The key from `config.yaml` to select the database configuration (e.g., `postgres_db`, `mysql_db`, `sqlite_db`).
    -   `MCP_TOOLBOX_URL`: (Optional) URL for the MCP Toolbox service.
    -   `CONNECTION_URL`: (Optional) A single database connection string. Can be used for a single-database setup.
    -   `DB_PASSWORD`: (Optional) Database password if not included in the connection string or YAML config.

2.  **YAML Configuration (`config.yaml`)**
    The `config.yaml` file defines the connection details for multiple databases. The agent uses the `DB_KEY` from the `.env` file to determine which database to connect to.

## Usage

To run the agent, you would typically execute a main Python script. The exact command depends on the project's entry point (e.g., `main.py`, `app.py`).

**Example Workflow:**

1.  Ensure your `.env` file is configured correctly. For instance, to use the PostgreSQL database defined in `config.yaml`. the `.env.example` contains all the environment variables you can


2. Run the register_db.py with your configured DB parameters:
    ```bash
    python utils/register_db.py 
    ```    

3. Run the toolbox server:
    #### Run on MacOS & Linux:
    ```bash
    ./toolbox --tools-file "tools.yaml"
    ```

    #### If installed via Homebrew on macOS/Linux:
    ```bash
    toolbox --tools-file "tools.yaml"
    ```

    ### Run on Windows (PowerShell or CMD):
    ```bash
    .\toolbox.exe --tools-file "tools.yaml"
    ``` 
     
4. Run the MCP based agent to query in natural language:
    ```bash
    python agent/mcp_toolbox_agent.py 
    ```

The agent will then:
1.  Read the `DB_KEY` from the environment.
2.  Load the corresponding database configuration from `config.yaml`.
3.  Connect to the PostgreSQL database.
4.  Use the table schema and your question to generate a SQL query via the LLM.
5.  Execute the query and return the result.
