# SQLSpec

## A Query Mapper for Python

SQLSpec is an experimental Python library designed to streamline and modernize your SQL interactions across a variety of database systems. While still in its early stages, SQLSpec aims to provide a flexible, typed, and extensible interface for working with SQL in Python.

**Note**: SQLSpec is currently under active development and the API is subject to change. It is not yet ready for production use. Contributions are welcome!

## Core Features (Current and Planned)

### Currently Implemented

- **Consistent Database Session Interface**: Provides a consistent connectivity interface for interacting with one or more database systems, including SQLite, Postgres, DuckDB, MySQL, Oracle, SQL Server, Spanner, BigQuery, and more.
- **Emphasis on RAW SQL and Minimal Abstractions**: SQLSpec is a library for working with SQL in Python. Its goals are to offer minimal abstractions between the user and the database. It does not aim to be an ORM library.
- **Type-Safe Queries**: Quickly map SQL queries to typed objects using libraries such as Pydantic, Msgspec, Attrs, etc.
- **Extensible Design**: Easily add support for new database dialects or extend existing functionality to meet your specific needs. Easily add support for async and sync database drivers.
- **Minimal Dependencies**: SQLSpec is designed to be lightweight and can run on its own or with other libraries such as `litestar`, `fastapi`, `flask` and more. (Contributions welcome!)
- **Support for Async and Sync Database Drivers**: SQLSpec supports both async and sync database drivers, allowing you to choose the style that best fits your application.

### Experimental Features (API will change rapidly)

- **SQL Builder API**: Type-safe query builder with method chaining (experimental and subject to significant changes)
- **Dynamic Query Manipulation**: Apply filters to pre-defined queries with a fluent API. Safely manipulate queries without SQL injection risk.
- **Dialect Validation and Conversion**: Use `sqlglot` to validate your SQL against specific dialects and seamlessly convert between them.
- **Storage Operations**: Direct export to Parquet, CSV, JSON with Arrow integration
- **Instrumentation**: OpenTelemetry and Prometheus metrics support
- **Basic Migration Management**: A mechanism to generate empty migration files where you can add your own SQL and intelligently track which migrations have been applied.

## What SQLSpec Is Not (Yet)

SQLSpec is a work in progress. While it offers a solid foundation for modern SQL interactions, it does not yet include every feature you might find in a mature ORM or database toolkit. The focus is on building a robust, flexible core that can be extended over time.

## Examples

We've talked about what SQLSpec is not, so let's look at what it can do.

These are just a few examples that demonstrate SQLSpec's flexibility. Each of the bundled adapters offers the same config and driver interfaces.

### Basic Usage

```python
from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig
from pydantic import BaseModel
# Create SQLSpec instance and configure database
sql = SQLSpec()
config = sql.add_config(SqliteConfig(database=":memory:"))

# Execute queries with automatic result mapping
with sql.provide_session(config) as session:
    # Simple query
    result = session.execute("SELECT 'Hello, SQLSpec!' as message")
    print(result.get_first())  # {'message': 'Hello, SQLSpec!'}
```

### SQL Builder Example (Experimental)

**Warning**: The SQL Builder API is highly experimental and will change significantly.

```python
from sqlspec import sql

# Build a simple query
query = sql.select("id", "name", "email").from_("users").where("active = ?", True)
print(query.build().sql)  # SELECT id, name, email FROM users WHERE active = ?

# More complex example with joins
query = (
    sql.select("u.name", "COUNT(o.id) as order_count")
    .from_("users u")
    .left_join("orders o", "u.id = o.user_id")
    .where("u.created_at > ?", "2024-01-01")
    .group_by("u.name")
    .having("COUNT(o.id) > ?", 5)
    .order_by("order_count", desc=True)
)

# Execute the built query
with sql.provide_session(config) as session:
    results = session.execute(query.build())
```

### DuckDB LLM

This is a quick implementation using some of the built-in Secret and Extension management features of SQLSpec's DuckDB integration.

It allows you to communicate with any compatible OpenAPI conversations endpoint (such as Ollama). This example:

- auto installs the `open_prompt` DuckDB extensions
- automatically creates the correct `open_prompt` compatible secret required to use the extension

```py
# /// script
# dependencies = [
#   "sqlspec[duckdb,performance]",
# ]
# ///
import os

from sqlspec import SQLSpec
from sqlspec.adapters.duckdb import DuckDBConfig
from pydantic import BaseModel

class ChatMessage(BaseModel):
    message: str

sql = SQLSpec()
etl_config = sql.add_config(
    DuckDBConfig(
        extensions=[{"name": "open_prompt"}],
        secrets=[
            {
                "secret_type": "open_prompt",
                "name": "open_prompt",
                "value": {
                    "api_url": "http://127.0.0.1:11434/v1/chat/completions",
                    "model_name": "gemma3:1b",
                    "api_timeout": "120",
                },
            }
        ],
    )
)
with sql.provide_session(etl_config) as session:
    result = session.select_one(
        "SELECT open_prompt(?)",
        "Can you write a haiku about DuckDB?",
        schema_type=ChatMessage
    )
    print(result) # result is a ChatMessage pydantic model
```

### DuckDB Gemini Embeddings

In this example, we are again using DuckDB. However, we are going to use the built-in to call the Google Gemini embeddings service directly from the database.

This example will:

- auto installs the `http_client` and `vss` (vector similarity search) DuckDB extensions
- when a connection is created, it ensures that the `generate_embeddings` macro exists in the DuckDB database
- Execute a simple query to call the Google API

```py
# /// script
# dependencies = [
#   "sqlspec[duckdb,performance]",
# ]
# ///
import os

from sqlspec import SQLSpec
from sqlspec.adapters.duckdb import DuckDBConfig

EMBEDDING_MODEL = "gemini-embedding-exp-03-07"
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY")
API_URL = (
    f"https://generativelanguage.googleapis.com/v1beta/models/{EMBEDDING_MODEL}:embedContent?key=${GOOGLE_API_KEY}"
)

sql = SQLSpec()
etl_config = sql.add_config(
    DuckDBConfig(
        extensions=[{"name": "vss"}, {"name": "http_client"}],
        on_connection_create=lambda connection: connection.execute(f"""
            CREATE IF NOT EXISTS MACRO generate_embedding(q) AS (
                WITH  __request AS (
                    SELECT http_post(
                        '{API_URL}',
                        headers => MAP {{
                            'accept': 'application/json',
                        }},
                        params => MAP {{
                            'model': 'models/{EMBEDDING_MODEL}',
                            'parts': [{{ 'text': q }}],
                            'taskType': 'SEMANTIC_SIMILARITY'
                        }}
                    ) AS response
                )
                SELECT *
                FROM __request,
            );
        """),
    )
)
with sql.provide_session(etl_config) as session:
    result = session.execute("SELECT generate_embedding('example text')")
    print(result.get_first()) # result is a dictionary when `schema_type` is omitted.
```

### Basic Litestar Integration

In this example we are going to demonstrate how to create a basic configuration that integrates into Litestar.

```py
# /// script
# dependencies = [
#   "sqlspec[aiosqlite]",
#   "litestar[standard]",
# ]
# ///

from litestar import Litestar, get

from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver
from sqlspec.extensions.litestar import DatabaseConfig, SQLSpec


@get("/")
async def simple_sqlite(db_session: AiosqliteDriver) -> dict[str, str]:
    return await db_session.select_one("SELECT 'Hello, world!' AS greeting")


sqlspec = SQLSpec(
    config=DatabaseConfig(
        config=AiosqliteConfig(),
        commit_mode="autocommit"
    )
)
app = Litestar(route_handlers=[simple_sqlite], plugins=[sqlspec])
```

## Inspiration and Future Direction

SQLSpec originally drew inspiration from features found in the `aiosql` library. This is a great library for working with and executing SQL stored in files. It's unclear how much of an overlap there will be between the two libraries, but it's possible that some features will be contributed back to `aiosql` where appropriate.

## Current Focus: Universal Connectivity

The primary goal at this stage is to establish a **native connectivity interface** that works seamlessly across all supported database environments. This means you can connect to any of the supported databases using a consistent API, regardless of the underlying driver or dialect.

## Adapters: Completed, In Progress, and Planned

This list is not final. If you have a driver you'd like to see added, please open an issue or submit a PR!

| Driver                                                                                                       | Database   | Mode    | Status     |
| :----------------------------------------------------------------------------------------------------------- | :--------- | :------ | :--------- |
| [`adbc`](https://arrow.apache.org/adbc/)                                                                     | Postgres   | Sync    | ✅         |
| [`adbc`](https://arrow.apache.org/adbc/)                                                                     | SQLite     | Sync    | ✅         |
| [`adbc`](https://arrow.apache.org/adbc/)                                                                     | Snowflake  | Sync    | ✅         |
| [`adbc`](https://arrow.apache.org/adbc/)                                                                     | DuckDB     | Sync    | ✅         |
| [`asyncpg`](https://magicstack.github.io/asyncpg/current/)                                                    | PostgreSQL | Async   | ✅         |
| [`psycopg`](https://www.psycopg.org/)                                                                         | PostgreSQL | Sync    | ✅         |
| [`psycopg`](https://www.psycopg.org/)                                                                         | PostgreSQL | Async   | ✅         |
| [`psqlpy`](https://psqlpy-python.github.io/)                                                                  | PostgreSQL | Async   | ✅        |
| [`aiosqlite`](https://github.com/omnilib/aiosqlite)                                                           | SQLite     | Async   | ✅         |
| `sqlite3`                                                                                                    | SQLite     | Sync    | ✅         |
| [`oracledb`](https://oracle.github.io/python-oracledb/)                                                      | Oracle     | Async   | ✅         |
| [`oracledb`](https://oracle.github.io/python-oracledb/)                                                      | Oracle     | Sync    | ✅         |
| [`duckdb`](https://duckdb.org/)                                                                               | DuckDB     | Sync    | ✅         |
| [`bigquery`](https://googleapis.dev/python/bigquery/latest/index.html)                                        | BigQuery   | Sync    | ✅ |
| [`spanner`](https://googleapis.dev/python/spanner/latest/index.html)                                         | Spanner    | Sync    | 🗓️  |
| [`sqlserver`](https://docs.microsoft.com/en-us/sql/connect/python/pyodbc/python-sql-driver-for-pyodbc?view=sql-server-ver16) | SQL Server | Sync    | 🗓️  |
| [`mysql`](https://dev.mysql.com/doc/connector-python/en/connector-python-api-mysql-connector-python.html)     | MySQL      | Sync    | 🗓️  |
| [`asyncmy`](https://github.com/long2ice/asyncmy)                                                           | MySQL      | Async   | ✅         |
| [`snowflake`](https://docs.snowflake.com)                                                                    | Snowflake  | Sync    | 🗓️  |

## Proposed Project Structure

- `sqlspec/`:
    - `adapters/`: Contains all database drivers and associated configuration.
    - `extensions/`:
        - `litestar/`: Litestar framework integration ✅
        - `fastapi/`: Future home of `fastapi` integration.
        - `flask/`: Future home of `flask` integration.
        - `*/`: Future home of your favorite framework integration
    - `base.py`: Contains base protocols for database configurations.
    - `statement/`: Contains the SQL statement system with builders, validation, and transformation.
    - `storage/`: Contains unified storage operations for data import/export.
    - `utils/`: Contains utility functions used throughout the project.
    - `exceptions.py`: Contains custom exceptions for SQLSpec.
    - `typing.py`: Contains type hints, type guards and several facades for optional libraries that are not required for the core functionality of SQLSpec.

## Get Involved

SQLSpec is an open-source project, and contributions are welcome! Whether you're interested in adding support for new databases, improving the query interface, or simply providing feedback, your input is valuable.

**Disclaimer**: SQLSpec is under active development. Expect changes and improvements as the project evolves.
