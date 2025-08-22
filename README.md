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

# Create SQLSpec instance and configure database
db_manager = SQLSpec()
config = SqliteConfig(pool_config={"database": ":memory:"}) # Thread local pooling
db_manager.add_config(config)

# Execute queries with automatic result mapping
with db_manager.provide_session(config) as session:
    # Simple query
    result = session.execute("SELECT 'Hello, SQLSpec!' as message")
    print(result.get_first())  # {'message': 'Hello, SQLSpec!'}

    # Type-safe single row query
    row = session.select_one("SELECT 'Hello, SQLSpec!' as message")
    print(row)  # {'message': 'Hello, SQLSpec!'}
```

### SQL Builder Example (Experimental)

**Warning**: The SQL Builder API is highly experimental and will change significantly.

```python
from sqlspec import sql

# Build a simple query
query = sql.select("id", "name", "email").from_("users").where("active = ?")
statement = query.to_statement()
print(statement.sql)  # SELECT id, name, email FROM users WHERE active = ?

# More complex example with joins
query = (
    sql.select("u.name", "COUNT(o.id) as order_count")
    .from_("users u")
    .left_join("orders o", "u.id = o.user_id")
    .where("u.created_at > ?")
    .group_by("u.name")
    .having("COUNT(o.id) > ?")
    .order_by("order_count", desc=True)
)

# Execute the built query with parameters
with db_manager.provide_session(config) as session:
    results = session.execute(query, "2024-01-01", 5)
```

### Type-Safe Result Mapping

SQLSpec supports automatic mapping to typed models using popular libraries:

```python
from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig
from pydantic import BaseModel

class User(BaseModel):
    id: int
    name: str
    email: str

db_manager = SQLSpec()
config = SqliteConfig(pool_config={"database": ":memory:"})
db_manager.add_config(config)

with db_manager.provide_session(config) as session:
    # Create and populate test data
    session.execute_script("""
        CREATE TABLE users (id INTEGER, name TEXT, email TEXT);
        INSERT INTO users VALUES (1, 'Alice', 'alice@example.com');
    """)
    # Map single result to typed model
    user = session.select_one("SELECT * FROM users WHERE id = ?", 1, schema_type=User)
    print(f"User: {user.name} ({user.email})")

    # Map multiple results
    users = session.select("SELECT * FROM users", schema_type=User)
    for user in users:
        print(f"User: {user.name}")
```

### Session Methods Overview

SQLSpec provides several convenient methods for executing queries:

```python
with db_manager.provide_session(config) as session:
    # Execute any SQL and get full result set
    result = session.execute("SELECT * FROM users")

    # Get single row (raises error if not found)
    user = session.select_one("SELECT * FROM users WHERE id = ?", 1)

    # Get single row or None (no error if not found)
    maybe_user = session.select_one_or_none("SELECT * FROM users WHERE id = ?", 999)

    # Execute with many parameter sets (bulk operations)
    session.execute_many(
        "INSERT INTO users (name, email) VALUES (?, ?)",
        [("Bob", "bob@example.com"), ("Carol", "carol@example.com")]
    )

    # Execute multiple statements as a script
    session.execute_script("""
        CREATE TABLE IF NOT EXISTS logs (id INTEGER, message TEXT);
        INSERT INTO logs (message) VALUES ('System started');
    """)
```

<details>
<summary>🦆 DuckDB LLM Integration Example</summary>

This is a quick implementation using some of the built-in Secret and Extension management features of SQLSpec's DuckDB integration.

It allows you to communicate with any compatible OpenAI conversations endpoint (such as Ollama). This example:

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

db_manager = SQLSpec()
config = DuckDBConfig(
    pool_config={"database": ":memory:"},
    driver_features={
        "extensions": [{"name": "open_prompt"}],
        "secrets": [
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
    },
)
db_manager.add_config(config)

with db_manager.provide_session(config) as session:
    result = session.select_one(
        "SELECT open_prompt(?)",
        "Can you write a haiku about DuckDB?",
        schema_type=ChatMessage
    )
    print(result) # result is a ChatMessage pydantic model
```

</details>

<details>
<summary>🔗 DuckDB Gemini Embeddings Example</summary>

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

db_manager = SQLSpec()
config = DuckDBConfig(
    pool_config={"database": ":memory:"},
    driver_features={
        "extensions": [{"name": "vss"}, {"name": "http_client"}],
        "on_connection_create": lambda connection: connection.execute(f"""
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
    },
)
db_manager.add_config(config)

with db_manager.provide_session(config) as session:
    result = session.execute("SELECT generate_embedding('example text')")
    print(result.get_first()) # result is a dictionary when `schema_type` is omitted.
```

</details>

### SQL File Loading

SQLSpec can load and manage SQL queries from files using aiosql-style named queries:

```python
from sqlspec import SQLSpec
from sqlspec.loader import SQLFileLoader
from sqlspec.adapters.sqlite import SqliteConfig

# Initialize with SQL file loader
db_manager = SQLSpec(loader=SQLFileLoader())
config = SqliteConfig(pool_config={"database": ":memory:"})
db_manager.add_config(config)

# Load SQL files from directory
db_manager.load_sql_files("./sql")

# SQL file: ./sql/users.sql
# -- name: get_user
# SELECT * FROM users WHERE id = ?
#
# -- name: create_user
# INSERT INTO users (name, email) VALUES (?, ?)

with db_manager.provide_session(config) as session:
    # Use named queries from files
    user = session.execute(db_manager.get_sql("get_user"), 1)
    session.execute(db_manager.get_sql("create_user"), "Alice", "alice@example.com")
```

### Database Migrations

SQLSpec includes a built-in migration system for managing schema changes. After configuring your database with migration settings, use the CLI commands:

```bash
# Initialize migration directory
sqlspec db init migrations

# Generate new migration file
sqlspec db make-migrations "Add user table"

# Apply all pending migrations
sqlspec db upgrade

# Show current migration status
sqlspec db show-current-revision
```

For Litestar applications, replace `sqlspec` with your application command:

```bash
# Using Litestar CLI integration
litestar db make-migrations "Add user table"
litestar db upgrade
litestar db show-current-revision
```

### Basic Litestar Integration

In this example we demonstrate how to create a basic configuration that integrates into Litestar:

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
        config=AiosqliteConfig(pool_config={"database": ":memory:"}), # built in local pooling
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

### Configuration Examples

Each adapter uses a consistent configuration pattern with `pool_config` for connection parameters:

```python
# SQLite
SqliteConfig(pool_config={"database": "/path/to/database.db"})
AiosqliteConfig(pool_config={"database": "/path/to/database.db"})  # Async
AdbcConfig(connection_config={"uri": "sqlite:///path/to/database.db"})  # ADBC

# PostgreSQL (multiple drivers available)
PsycopgSyncConfig(pool_config={"host": "localhost", "database": "mydb", "user": "user", "password": "pass"})
PsycopgAsyncConfig(pool_config={"host": "localhost", "database": "mydb", "user": "user", "password": "pass"})  # Async
AsyncpgConfig(pool_config={"host": "localhost", "database": "mydb", "user": "user", "password": "pass"})
PsqlpyConfig(pool_config={"dsn": "postgresql://user:pass@localhost/mydb"})
AdbcConfig(connection_config={"uri": "postgresql://user:pass@localhost/mydb"})  # ADBC

# DuckDB
DuckDBConfig(pool_config={"database": ":memory:"})  # or file path
AdbcConfig(connection_config={"uri": "duckdb:///path/to/database.duckdb"})  # ADBC

# MySQL
AsyncmyConfig(pool_config={"host": "localhost", "database": "mydb", "user": "user", "password": "pass"})  # Async

# Oracle
OracleSyncConfig(pool_config={"host": "localhost", "service_name": "XEPDB1", "user": "user", "password": "pass"})
OracleAsyncConfig(pool_config={"host": "localhost", "service_name": "XEPDB1", "user": "user", "password": "pass"})  # Async

# BigQuery
BigQueryConfig(pool_config={"project": "my-project", "dataset": "my_dataset"})
AdbcConfig(connection_config={"driver_name": "adbc_driver_bigquery", "project_id": "my-project", "dataset_id": "my_dataset"})  # ADBC
```

### Supported Drivers

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

## Project Structure

- `sqlspec/`:
    - `adapters/`: Database-specific drivers and configuration classes for all supported databases
    - `extensions/`: Framework integrations and external library adapters
        - `litestar/`: Litestar web framework integration with dependency injection ✅
        - `aiosql/`: Integration with aiosql for SQL file loading ✅
        - Future integrations: `fastapi/`, `flask/`, etc.
    - `builder/`: Fluent SQL query builder with method chaining and type safety
        - `mixins/`: Composable query building operations (WHERE, JOIN, ORDER BY, etc.)
    - `core/`: Core query processing infrastructure
        - `statement.py`: SQL statement wrapper with metadata and type information
        - `parameters.py`: Parameter style conversion and validation
        - `result.py`: Result set handling and type mapping
        - `compiler.py`: SQL compilation and validation using SQLGlot
        - `cache.py`: Statement caching for performance optimization
    - `driver/`: Base driver system with sync/async support and transaction management
        - `mixins/`: Shared driver capabilities (result processing, SQL translation)
    - `migrations/`: Database migration system with CLI commands
    - `storage/`: Unified data import/export operations with multiple backends
        - `backends/`: Storage backend implementations (fsspec, obstore)
    - `utils/`: Utility functions, type guards, and helper tools
    - `base.py`: Main SQLSpec registry and configuration manager
    - `loader.py`: SQL file loading system for `.sql` files
    - `cli.py`: Command-line interface for migrations and database operations
    - `config.py`: Base configuration classes and protocols
    - `protocols.py`: Type protocols for runtime type checking
    - `exceptions.py`: Custom exception hierarchy for SQLSpec
    - `typing.py`: Type definitions, guards, and optional dependency facades

## Get Involved

SQLSpec is an open-source project, and contributions are welcome! Whether you're interested in adding support for new databases, improving the query interface, or simply providing feedback, your input is valuable.

**Disclaimer**: SQLSpec is under active development. Expect changes and improvements as the project evolves.
