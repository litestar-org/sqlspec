# Google ADK Integration

`sqlspec.extensions.adk` integrates SQLSpec with [Google Agent Development Kit (`google-adk >= 2.10.0`)](https://google.github.io/adk-docs/), providing persistent database-backed implementations of ADK's `BaseSessionService` (`SQLSpecSessionService`), `BaseMemoryService` (`SQLSpecMemoryService`), and `BaseArtifactService` (`SQLSpecArtifactService`) across supported database adapters.

## Installation

```bash
uv add "sqlspec[adk]"
```

## Architecture and Store Services

| ADK Service | SQLSpec Class | Adapter Store Base | Responsibility |
| --- | --- | --- | --- |
| **Session & Event Store** | `SQLSpecSessionService` | `BaseAsyncADKStore` / `BaseSyncADKStore` | Persists conversation sessions, structured state (`app:`, `user:`, session state), and ordered `Event` records with optimistic revision checks and per-session concurrency locks. |
| **Long-Term Memory Store** | `SQLSpecMemoryService` | `BaseAsyncADKMemoryStore` / `BaseSyncADKMemoryStore` | Indexes conversation turns and tool outputs into `adk_memory_entries` with substring (`simple`), full-text (`fts`), vector (`vector`), and Reciprocal Rank Fusion (`hybrid`) search. |
| **Artifact Store** | `SQLSpecArtifactService` | `BaseAsyncADKArtifactStore` / `BaseSyncADKArtifactStore` | Stores versioned binary and text artifacts scoped by `(app_name, user_id, session_id, filename, version)` with `user:`-prefixed cross-session sharing and `artifact://` resolution. |

## Configuring `extension_config["adk"]`

Configure table names, full-text indexing, vector dimensions, and optional owner foreign keys on any adapter via `extension_config["adk"]` (`ADKConfig`):

```python
--8<-- "docs/examples/extensions/adk/backend_config.py:example"
```

### Key `ADKConfig` Options

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `session_table` | `str` | `"adk_sessions"` | Table storing session metadata and serialized state JSON. |
| `events_table` | `str` | `"adk_events"` | Table storing ordered conversation events linked to `session_table`. |
| `memory_table` | `str` | `"adk_memory_entries"` | Table storing searchable long-term memory entries. |
| `artifact_table` | `str` | `"adk_artifacts"` | Table storing versioned binary/text agent artifacts. |
| `memory_use_fts` | `bool` | `False` | Enables native full-text search indexing (`FTS5` on SQLite, `tsvector`/GIN or `ParadeDB` BM25 on PostgreSQL, `CONTAINS` on Oracle Text, `FULLTEXT` on MySQL/MSSQL, `SEARCH` on Spanner). |
| `memory_use_vector` | `bool` | `False` | Adds vector embedding storage and index DDL (`pgvector`, Oracle 26ai `VECTOR`, Spanner `ARRAY<FLOAT32>`, DuckDB `FLOAT[N]`). |
| `memory_vector_dimensions` | `int` | `768` | Embedding vector dimensionality when `memory_use_vector=True`. |
| `owner_id_column` | `str | None` | `None` | Optional DDL definition (for example, `"tenant_id UUID NOT NULL REFERENCES tenants(id)"`) linking sessions to an application table. |

## Persisting Sessions and Events

Instantiate the adapter's `*ADKStore` (such as `AiosqliteADKStore`, `AsyncpgADKStore`, `OracleAsyncADKStore`, or `SpannerAsyncADKStore`), call `await store.ensure_tables()`, and pass the store to `SQLSpecSessionService(store)` or call the store directly:

```python
--8<-- "docs/examples/extensions/adk/memory_store.py:example"
```

When wiring an ADK `Runner`, pass `SQLSpecSessionService(store)`, `SQLSpecMemoryService(memory_store)`, and `SQLSpecArtifactService(artifact_store)` inside an async context manager (`async with SQLSpecSessionService(store) as session_service:`) so session locks and resources close cleanly on shutdown.

## Full-Text, Vector, and Hybrid RRF Memory Search

`ADKMemoryStore` supports four search modes across engines:

1. **Simple (`simple`)**: Portable case-insensitive substring matching across all ADK-enabled adapters.
2. **Full-Text (`fts`)**: Native lexical ranking (`FTS5` on `sqlite`/`aiosqlite`, `tsvector` or ParadeDB `@@@` BM25 on PostgreSQL, Oracle Text `CONTAINS` with `"AND"`, `"OR"`, `"ACCUM"`, and `"NEAR"` operators, `MATCH ... AGAINST` on MySQL, and `SEARCH`/`SCORE` on Spanner).
3. **Vector (`vector`)**: Cosine or inner-product similarity search on `asyncpg`, `psycopg`, `psqlpy`, `adbc` (`pgvector`), `oracledb` (Oracle 26ai HNSW `FETCH APPROX FIRST` or exact top-K), `spanner` (`APPROX_COSINE_DISTANCE`), and `duckdb` (`array_cosine_similarity`).
4. **Hybrid RRF (`hybrid`)**: Single-round-trip Reciprocal Rank Fusion (`1 / (k + rank)`) combining vector distance and full-text rank in one SQL CTE on PostgreSQL (`asyncpg`, `psycopg`, `psqlpy`, `adbc`), Oracle 26ai (`oracledb`), Cloud Spanner (`spanner`), and DuckDB (`duckdb`).

```python
--8<-- "docs/examples/extensions/adk/tool_integration.py:example"
```

## Production Domain AI Runtime Wiring

In production applications, structure agent stores within an `app/domain/ai/runtime/` service factory, enabling `"adk"` extension migrations so revisions are tracked automatically by the CLI:

```python
"""Domain AI runtime wiring for Google ADK stores."""

from pathlib import Path

from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.adapters.asyncpg.adk import (
    AsyncpgADKArtifactStore,
    AsyncpgADKMemoryStore,
    AsyncpgADKStore,
)
from sqlspec.extensions.adk import (
    SQLSpecArtifactService,
    SQLSpecMemoryService,
    SQLSpecSessionService,
)

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent


def create_adk_services(
    spec: SQLSpec,
) -> tuple[SQLSpecSessionService, SQLSpecMemoryService, SQLSpecArtifactService]:
    """Wire ADK stores and service adapters on an AsyncpgConfig instance."""
    config = spec.add_config(
        AsyncpgConfig(
            bind_key="ai",
            connection_config={"dsn": "postgresql://localhost/app"},
            migration_config={
                "script_location": str(BASE_DIR / "db" / "migrations"),
                "include_extensions": ["adk"],
            },
            extension_config={
                "adk": {
                    "session_table": "adk_sessions",
                    "events_table": "adk_events",
                    "memory_table": "adk_memory_entries",
                    "artifact_table": "adk_artifacts",
                    "memory_use_fts": True,
                    "memory_use_vector": True,
                    "memory_vector_dimensions": 768,
                }
            },
        )
    )

    session_store = AsyncpgADKStore(config)
    memory_store = AsyncpgADKMemoryStore(config)
    artifact_store = AsyncpgADKArtifactStore(config)

    return (
        SQLSpecSessionService(session_store),
        SQLSpecMemoryService(memory_store),
        SQLSpecArtifactService(artifact_store),
    )
```

## Schema Migrations and CLI Maintenance

Instead of calling `ensure_tables()` at runtime in production, enable `"adk"` in `migration_config={"enable_extensions": ["adk"]}` so `sqlspec upgrade` tracks ADK schema revisions alongside your application migrations.

Use the built-in `sqlspec adk memory` CLI commands to inspect, clean up, or verify memory stores:

```bash
sqlspec --config myapp.db.spec adk memory stats
sqlspec --config myapp.db.spec adk memory cleanup --older-than-days 90
```

## Recap

- Configure ADK tables, FTS, and vector search in `extension_config["adk"]`.
- Pair `SQLSpecSessionService`, `SQLSpecMemoryService`, and `SQLSpecArtifactService` with your adapter's `adk` store classes.
- See the [Database Capability Matrix](../databases/index.md) for per-adapter ADK vector/FTS support and the [ADK Extension Reference](/reference/extensions/adk/) for full API signatures.
