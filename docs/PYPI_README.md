# SQLSpec

<<<<<<< HEAD
**Type-safe SQL query mapper with minimal abstraction between Python and SQL.**
||||||| parent of 4cb0363 (chore(release): bump to `v0.30.0`)
## A Query Mapper for Python
=======
**Type-safe SQL execution layer for Python.**
>>>>>>> 4cb0363 (chore(release): bump to `v0.30.0`)

<<<<<<< HEAD
SQLSpec keeps you close to the SQL you already write while providing typed
results, automatic parameter handling, and a unified driver interface across
popular databases (PostgreSQL, SQLite, DuckDB, MySQL, Oracle, BigQuery, and
more). It is not an ORM. Think of it as a connectivity and query mapping layer
that favors raw SQL, observability, and predictable behavior.
||||||| parent of 4cb0363 (chore(release): bump to `v0.30.0`)
SQLSpec is an experimental Python library designed to streamline and modernize your SQL interactions across a variety of database systems. While still in its early stages, SQLSpec aims to provide a flexible, typed, and extensible interface for working with SQL in Python.
=======
SQLSpec handles database connectivity and result mapping so you can focus on SQL. Write raw queries when you need precision, use the builder API when you need composability, or load SQL from files when you need organization. Every statement passes through a [sqlglot](https://github.com/tobymao/sqlglot)-powered AST pipeline for validation, dialect conversion, and optimization before execution. Export results as Python objects, Arrow tables, Polars or pandas DataFrames.
>>>>>>> 4cb0363 (chore(release): bump to `v0.30.0`)

<<<<<<< HEAD
## Status
||||||| parent of 4cb0363 (chore(release): bump to `v0.30.0`)
**Note**: SQLSpec is currently under active development and the API is subject to change. It is not yet ready for production use. Contributions are welcome!
=======
It's not an ORM. It's the connectivity and processing layer between your application and your database that provides the right abstraction for each situation without dictating how you write SQL.
>>>>>>> 4cb0363 (chore(release): bump to `v0.30.0`)

<<<<<<< HEAD
SQLSpec is currently in active development. The public API may change at
any time and production use is not yet recommended. Follow the
[docs](https://sqlspec.dev/) and changelog for updates.
||||||| parent of 4cb0363 (chore(release): bump to `v0.30.0`)
## Core Features (Current and Planned)
=======
## Status
>>>>>>> 4cb0363 (chore(release): bump to `v0.30.0`)

<<<<<<< HEAD
## Highlights
||||||| parent of 4cb0363 (chore(release): bump to `v0.30.0`)
### Currently Implemented
=======
SQLSpec is currently in active development. The public API may change. Follow the [docs](https://sqlspec.dev/) and changelog for updates.
>>>>>>> 4cb0363 (chore(release): bump to `v0.30.0`)

<<<<<<< HEAD
- **SQL first**: Validate and execute the SQL you write, with helpers for
  statement stacks, SQL file loading, and dialect-aware compilation.
- **SQL AST pipeline**: Every statement is processed by the `sqlglot` library for validation, dialect tuning, and caching before it ever hits the driver.
- **Unified connectivity**: One session API for sync and async drivers across
  a growing list of adapters (psycopg, asyncpg, aiosqlite, DuckDB, BigQuery,
  Oracle, asyncmy, ADBC, and more).
- **Typed results**: Map rows directly into Pydantic, Msgspec, attrs, or
  dataclasses for predictable data structures.
- **Statement stack + builder**: Compose multi-statement workloads, stream
  them through the stack observer pipeline, and rely on Arrow export support
  across every driver when you need columnar results.
- **SQL file loading**: Ship named queries alongside your code and load them
  aiosql-style with observability, caching, and parameter validation baked in.
- **Framework integrations**: Litestar plugin with automatic dependency
  injection plus extension points for FastAPI, Starlette, and others.
- **Observability ready**: Built-in instrumentation hooks for OpenTelemetry
  and Prometheus, plus structured logging guidance.
||||||| parent of 4cb0363 (chore(release): bump to `v0.30.0`)
- **Consistent Database Session Interface**: Provides a consistent connectivity interface for interacting with one or more database systems, including SQLite, Postgres, DuckDB, MySQL, Oracle, SQL Server, Spanner, BigQuery, and more.
- **Emphasis on RAW SQL and Minimal Abstractions**: SQLSpec is a library for working with SQL in Python. Its goals are to offer minimal abstractions between the user and the database. It does not aim to be an ORM library.
- **Type-Safe Queries**: Quickly map SQL queries to typed objects using libraries such as Pydantic, Msgspec, Attrs, etc.
- **Extensible Design**: Easily add support for new database dialects or extend existing functionality to meet your specific needs. Easily add support for async and sync database drivers.
- **Framework Extensions**: First-class integrations for Litestar, Starlette, and FastAPI with automatic transaction handling and lifecycle management
- **Support for Async and Sync Database Drivers**: SQLSpec supports both async and sync database drivers, allowing you to choose the style that best fits your application.
=======
## What You Get
>>>>>>> 4cb0363 (chore(release): bump to `v0.30.0`)

<<<<<<< HEAD
## Quick Start
||||||| parent of 4cb0363 (chore(release): bump to `v0.30.0`)
### Experimental Features (API will change rapidly)
=======
**Connection Management**
>>>>>>> 4cb0363 (chore(release): bump to `v0.30.0`)

<<<<<<< HEAD
### Install
||||||| parent of 4cb0363 (chore(release): bump to `v0.30.0`)
- **SQL Builder API**: Type-safe query builder with method chaining (experimental and subject to significant changes)
- **Dynamic Query Manipulation**: Apply filters to pre-defined queries with a fluent API. Safely manipulate queries without SQL injection risk.
- **Dialect Validation and Conversion**: Use `sqlglot` to validate your SQL against specific dialects and seamlessly convert between them.
- **Storage Operations**: Direct export to Parquet, CSV, JSON with Arrow integration
- **Instrumentation**: OpenTelemetry and Prometheus metrics support
- **Basic Migration Management**: A mechanism to generate empty migration files where you can add your own SQL and intelligently track which migrations have been applied.
=======
- Connection pooling with configurable size, timeout, and lifecycle hooks
- Sync and async support with a unified API surface
- Adapters for PostgreSQL (psycopg, asyncpg, psqlpy), SQLite (sqlite3, aiosqlite), DuckDB, MySQL (asyncmy), Oracle, BigQuery, and ADBC-compatible databases
>>>>>>> 4cb0363 (chore(release): bump to `v0.30.0`)

<<<<<<< HEAD
```bash
pip install "sqlspec[sqlite]"
```
||||||| parent of 4cb0363 (chore(release): bump to `v0.30.0`)
## What SQLSpec Is Not (Yet)
=======
**Query Execution**
>>>>>>> 4cb0363 (chore(release): bump to `v0.30.0`)

<<<<<<< HEAD
||||||| parent of 4cb0363 (chore(release): bump to `v0.30.0`)
SQLSpec is a work in progress. While it offers a solid foundation for modern SQL interactions, it does not yet include every feature you might find in a mature ORM or database toolkit. The focus is on building a robust, flexible core that can be extended over time.

## Examples

We've talked about what SQLSpec is not, so let's look at what it can do.

These are just a few examples that demonstrate SQLSpec's flexibility. Each of the bundled adapters offers the same config and driver interfaces.

### Basic Usage
=======
- Raw SQL strings with automatic parameter binding and dialect translation
- SQL AST parsing via sqlglot for validation, optimization, and dialect conversion
- Builder API for programmatic query construction without string concatenation
- SQL file loading to keep queries organized alongside your code (aiosql-style)
- Statement stacks for batching multiple operations with transaction control

**Result Handling**

- Type-safe result mapping to Pydantic, msgspec, attrs, or dataclasses
- Apache Arrow export for zero-copy integration with pandas, Polars, and analytical tools
- Result iteration, single-row fetch, or bulk retrieval based on your use case

**Framework Integration**

- Litestar plugin with dependency injection for connections, sessions, and pools
- Starlette/FastAPI middleware for automatic transaction management
- Flask extension with sync/async portal support

**Production Features**

- SQL validation and caching via sqlglot AST parsing
- OpenTelemetry and Prometheus instrumentation hooks
- Structured logging with correlation ID support
- Migration CLI for schema versioning

## Quick Start

### Install

```bash
pip install "sqlspec"
```

>>>>>>> 4cb0363 (chore(release): bump to `v0.30.0`)
### Run your first query

```python
from pydantic import BaseModel
<<<<<<< HEAD

||||||| parent of 4cb0363 (chore(release): bump to `v0.30.0`)
=======
>>>>>>> 4cb0363 (chore(release): bump to `v0.30.0`)
from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig

<<<<<<< HEAD

||||||| parent of 4cb0363 (chore(release): bump to `v0.30.0`)
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
=======
>>>>>>> 4cb0363 (chore(release): bump to `v0.30.0`)
class Greeting(BaseModel):
    message: str

<<<<<<< HEAD
||||||| parent of 4cb0363 (chore(release): bump to `v0.30.0`)
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
=======
spec = SQLSpec()
db = sql.add_config(SqliteConfig(pool_config={"database": ":memory:"}))
>>>>>>> 4cb0363 (chore(release): bump to `v0.30.0`)

<<<<<<< HEAD
sql = SQLSpec()
sqlite_db = sql.add_config(SqliteConfig(pool_config={"database": ":memory:"}))

with sql.provide_session(sqlite_db) as session:
||||||| parent of 4cb0363 (chore(release): bump to `v0.30.0`)
with db_manager.provide_session(config) as session:
    result = session.select_one(
        "SELECT open_prompt(?)",
        "Can you write a haiku about DuckDB?",
        schema_type=ChatMessage
=======
with spec.provide_session(db) as session:
>>>>>>> 4cb0363 (chore(release): bump to `v0.30.0`)
    greeting = session.select_one(
        "SELECT 'Hello, SQLSpec!' AS message",
        schema_type=Greeting,
    )
<<<<<<< HEAD
    print(greeting.message)
||||||| parent of 4cb0363 (chore(release): bump to `v0.30.0`)
    print(result) # result is a ChatMessage pydantic model
=======
    print(greeting.message)  # Output: Hello, SQLSpec!
>>>>>>> 4cb0363 (chore(release): bump to `v0.30.0`)
```

<<<<<<< HEAD
Explore the [Getting Started guide](https://sqlspec.dev/getting_started/)
for installation variants, driver selection, and typed result mapping.
||||||| parent of 4cb0363 (chore(release): bump to `v0.30.0`)
</details>
=======
That's it. Write SQL, define a schema, get typed objects back. Connection pooling, parameter binding, and result mapping are handled automatically.
>>>>>>> 4cb0363 (chore(release): bump to `v0.30.0`)

<<<<<<< HEAD
## Documentation
||||||| parent of 4cb0363 (chore(release): bump to `v0.30.0`)
<details>
<summary>🔗 DuckDB Gemini Embeddings Example</summary>
=======
See the [Getting Started guide](https://sqlspec.dev/getting_started/) for installation variants, adapter selection, and advanced result mapping options.
>>>>>>> 4cb0363 (chore(release): bump to `v0.30.0`)

<<<<<<< HEAD
- [Getting Started](https://sqlspec.dev/getting_started/)
- [Usage Guides](https://sqlspec.dev/usage/)
- [Examples Gallery](https://sqlspec.dev/examples/)
- [API Reference](https://sqlspec.dev/reference/)
- [CLI Reference](https://sqlspec.dev/usage/cli.html)
||||||| parent of 4cb0363 (chore(release): bump to `v0.30.0`)
In this example, we are again using DuckDB. However, we are going to use the built-in to call the Google Gemini embeddings service directly from the database.
=======
## Documentation
>>>>>>> 4cb0363 (chore(release): bump to `v0.30.0`)

<<<<<<< HEAD
## Ecosystem Snapshot
||||||| parent of 4cb0363 (chore(release): bump to `v0.30.0`)
This example will:
=======
- [Getting Started](https://sqlspec.dev/getting_started/)
- [Usage Guides](https://sqlspec.dev/usage/)
- [Examples Gallery](https://sqlspec.dev/examples/)
- [API Reference](https://sqlspec.dev/reference/)
- [CLI Reference](https://sqlspec.dev/usage/cli.html)
>>>>>>> 4cb0363 (chore(release): bump to `v0.30.0`)

<<<<<<< HEAD
- **Adapters**: PostgreSQL (psycopg, asyncpg, psqlpy), SQLite (sqlite3,
  aiosqlite), DuckDB (native + ADBC), MySQL (asyncmy), Oracle (oracledb),
  BigQuery, Snowflake, and additional ADBC targets.
- **Extensions**: Litestar integration, SQL file loader, storage backends,
  telemetry observers, and experimental SQL builder.
- **Tooling**: Migration CLI, stack execution observers, driver parameter
  profiles, and Arrow-friendly storage helpers.
||||||| parent of 4cb0363 (chore(release): bump to `v0.30.0`)
- auto installs the `http_client` and `vss` (vector similarity search) DuckDB extensions
- when a connection is created, it ensures that the `generate_embeddings` macro exists in the DuckDB database
- Execute a simple query to call the Google API
=======
## Reference Applications
>>>>>>> 4cb0363 (chore(release): bump to `v0.30.0`)

<<<<<<< HEAD
See the [usage docs](https://sqlspec.dev/usage/) for the latest adapter matrix,
configuration patterns, and feature deep dives—including the
[SQL file loader guide](https://sqlspec.dev/usage/loader.html).
||||||| parent of 4cb0363 (chore(release): bump to `v0.30.0`)
```py
# /// script
# dependencies = [
#   "sqlspec[duckdb,performance]",
# ]
# ///
import os
=======
- **[PostgreSQL + Vertex AI Demo](https://github.com/cofin/postgres-vertexai-demo)** - Vector search with pgvector and real-time chat using Litestar and Google ADK. Shows connection pooling, migrations, type-safe result mapping, vector embeddings, and response caching.
- **[Oracle + Vertex AI Demo](https://github.com/cofin/oracledb-vertexai-demo)** - Oracle 23ai vector search with semantic similarity using HNSW indexes. Demonstrates NumPy array conversion, large object (CLOB) handling, and real-time performance metrics.
>>>>>>> 4cb0363 (chore(release): bump to `v0.30.0`)

<<<<<<< HEAD
## Contributing
||||||| parent of 4cb0363 (chore(release): bump to `v0.30.0`)
from sqlspec import SQLSpec
from sqlspec.adapters.duckdb import DuckDBConfig
=======
See the [usage docs](https://sqlspec.dev/usage/) for detailed guides on adapters, configuration patterns, and features like the [SQL file loader](https://sqlspec.dev/usage/loader.html).
>>>>>>> 4cb0363 (chore(release): bump to `v0.30.0`)

<<<<<<< HEAD
Contributions, issue reports, and adapter ideas are welcome. Review the
[contributor guide](https://sqlspec.dev/contributing/) and follow the project
coding standards before opening a pull request.
||||||| parent of 4cb0363 (chore(release): bump to `v0.30.0`)
EMBEDDING_MODEL = "gemini-embedding-exp-03-07"
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY")
API_URL = (
    f"https://generativelanguage.googleapis.com/v1beta/models/{EMBEDDING_MODEL}:embedContent?key=${GOOGLE_API_KEY}"
)
=======
## Built With
>>>>>>> 4cb0363 (chore(release): bump to `v0.30.0`)

<<<<<<< HEAD
## License
||||||| parent of 4cb0363 (chore(release): bump to `v0.30.0`)
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
=======
- **[sqlglot](https://github.com/tobymao/sqlglot)** - SQL parser, transpiler, and optimizer powering SQLSpec's AST pipeline
>>>>>>> 4cb0363 (chore(release): bump to `v0.30.0`)

<<<<<<< HEAD
||||||| parent of 4cb0363 (chore(release): bump to `v0.30.0`)
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
sqlspec --config myapp.config init

# Generate new migration file
sqlspec --config myapp.config create-migration -m "Add user table"

# Apply all pending migrations
sqlspec --config myapp.config upgrade

# Show current migration status
sqlspec --config myapp.config show-current-revision
```

For Litestar applications, replace `sqlspec` with your application command:

```bash
# Using Litestar CLI integration
litestar database create-migration -m "Add user table"
litestar database upgrade
litestar database show-current-revision
```

### Shell Completion

SQLSpec CLI supports tab completion for bash, zsh, and fish shells. Enable it with:

```bash
# Bash - add to ~/.bashrc
eval "$(_SQLSPEC_COMPLETE=bash_source sqlspec)"

# Zsh - add to ~/.zshrc
eval "$(_SQLSPEC_COMPLETE=zsh_source sqlspec)"

# Fish - add to ~/.config/fish/completions/sqlspec.fish
eval (env _SQLSPEC_COMPLETE=fish_source sqlspec)
```

After setup, you can tab-complete commands and options:

```bash
sqlspec <TAB>         # Shows: create-migration, downgrade, init, ...
sqlspec upgrade --<TAB>  # Shows: --bind-key, --help, --no-prompt, ...
```

See the [CLI documentation](https://sqlspec.litestar.dev/usage/cli.html) for complete setup instructions.

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
from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver
from sqlspec.extensions.litestar import SQLSpecPlugin

@get("/")
async def simple_sqlite(db_session: AiosqliteDriver) -> dict[str, str]:
    return await db_session.select_one("SELECT 'Hello, world!' AS greeting")


sqlspec = SQLSpec()
sqlspec.add_config(AiosqliteConfig(pool_config={"database": ":memory:"}))
app = Litestar(route_handlers=[simple_sqlite], plugins=[SQLSpecPlugin(sqlspec)])
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
=======
## Contributing

Contributions, issue reports, and adapter ideas are welcome. Review the
[contributor guide](https://sqlspec.dev/contributing/) and follow the project
coding standards before opening a pull request.

## License

>>>>>>> 4cb0363 (chore(release): bump to `v0.30.0`)
SQLSpec is distributed under the MIT License.
