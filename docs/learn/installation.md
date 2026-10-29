# Installation

SQLSpec targets **Python 3.11 and newer** (`3.11` through `3.15`). Installing the base `sqlspec` package pulls in minimal pure-Python dependencies (`sqlglot`, `rich-click`, `typing-extensions`, and `mypy-extensions`) and includes full support for SQLite via Python's standard library `sqlite3` module.

## Install Base Package

=== "uv"

    ```bash
    uv add sqlspec
    ```

=== "pip"

    ```bash
    pip install sqlspec
    ```

Combine multiple optional extras in a single install command when adding drivers, serializers, or framework plugins:

=== "uv"

    ```bash
    uv add "sqlspec[asyncpg,msgspec,litestar,opentelemetry]"
    ```

=== "pip"

    ```bash
    pip install "sqlspec[asyncpg,msgspec,litestar,opentelemetry]"
    ```

## Database Driver Extras

Install only the driver extras needed by your deployment target. See [Databases](/databases/) for adapter-by-adapter connection and pool configuration.

| Family | Extra | Installed Driver Package(s) |
| --- | --- | --- |
| **SQLite** | *(built-in)* / `aiosqlite` | Standard library `sqlite3` (sync) or `aiosqlite` (async) |
| **PostgreSQL** | `asyncpg`, `psycopg`, `psqlpy` | `asyncpg`, `psycopg[binary,pool]`, or `psqlpy>=0.12.1` |
| **Managed Cloud Postgres** | `alloydb`, `cloud-sql` | `google-cloud-alloydb-connector`, `cloud-sql-python-connector` |
| **CockroachDB** | `cockroachdb` | `psycopg[binary,pool]` and `asyncpg` |
| **MySQL / MariaDB** | `asyncmy`, `aiomysql`, `pymysql`, `mysql-connector` | `asyncmy>=0.2.13`, `aiomysql`, `pymysql`, or `mysql-connector-python` |
| **Oracle** | `oracledb` | `oracledb>=3.4` (Thin mode built-in; optional Instant Client for Thick mode) |
| **SQL Server** | `mssql-python`, `pymssql` | `mssql-python>=1.13` or `pymssql` |
| **IBM Db2** | `db2` | `ibm_db>=3.3.0` |
| **DuckDB** | `duckdb` | `duckdb`, `pyarrow`, `pytz` |
| **Google Cloud** | `bigquery`, `spanner` | `google-cloud-bigquery` (+ Storage/GCS) or `google-cloud-spanner>=3.64.0` |
| **Arrow Flight / ADBC / ODBC** | `adbc`, `arrow-odbc` | `adbc_driver_manager` + `pyarrow` (pair with a PyPI ADBC driver) or `arrow-odbc>=10.4` |

## Web and AI Framework Extras

See [Integrations](/integrations/) for plugin wiring, request-scoped sessions, and store setup.

| Extra | Target Integration |
| --- | --- |
| `litestar` | `SQLSpecPlugin`, DI providers, `FilterConfig` query dependencies, and session stores (`litestar>=2.23.0`) |
| `adk` | Google Agent Development Kit session, memory, and artifact services (`google-adk>=2.10.0`) |
| `fastapi` | FastAPI request lifespan and `Depends` session providers |
| `starlette` | Starlette middleware and `request.state` session management |
| `flask` | Flask `teardown_appcontext` and `g`-scoped synchronous sessions |
| `sanic` | Sanic `before_server_start` / middleware async session lifecycle |

For Dishka dependency injection, install `dishka` alongside `sqlspec` and see the [Dishka integration guide](/integrations/dishka/).

## Schema, Serialization, and Identifier Extras

| Extra | Purpose |
| --- | --- |
| `msgspec` | Zero-overhead `msgspec.Struct` row hydration and fast JSON encoding/decoding |
| `pydantic` | `pydantic.BaseModel` and `pydantic-extra-types` row validation |
| `attrs` | `attrs` + `cattrs` structured row hydration |
| `orjson` | Optional `orjson` serializer backend |
| `uuid`, `nanoid` | `uuid-utils>=0.17.0` (UUIDv7) and `fastnanoid>=0.4.1` identifier generation |
| `performance` | Bundled `msgspec` and `librt>=0.10.0` runtime acceleration helpers |

## Analytics, Storage, Observability, and Compiled Wheels

| Extra | Purpose |
| --- | --- |
| `polars`, `pandas` | DataFrame conversion helpers over `select_to_arrow` (`polars` + `pyarrow` or `pandas` + `pyarrow`) |
| `obstore`, `fsspec` | Object storage backends for S3, GCS, Azure Blob, and HTTP filesystems in `StorageRegistry` |
| `opentelemetry` | `opentelemetry-instrumentation` span emission for driver execution |
| `prometheus` | `prometheus-client` query counter and latency histogram metrics |
| `mypyc` | Enables `sqlglot[c]>=30.13.0` alongside SQLSpec's pre-compiled `mypyc` C-extension wheels |

## Verify Your Installation

Run a quick one-liner to verify `sqlspec` and the built-in SQLite adapter:

```bash
uv run python -c "import sqlspec; print(sqlspec.__version__)"
```

Continue to the [Quickstart](quickstart.md) to run synchronous, asynchronous, and typed queries.
