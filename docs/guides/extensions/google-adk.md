# Google ADK Extension Guide

Describes how to persist Google Agent Development Kit (ADK) sessions and events with SQLSpec-backed stores, covering configuration, adapter selection, and operational patterns.

## Quick Facts

- Install with `pip install "sqlspec[asyncpg] google-genai"` (swap the adapter extra for your database).
- `SQLSpecSessionService` implements `BaseSessionService` and delegates all storage to adapter-specific stores.
- Stores live in `sqlspec.adapters.<adapter>.adk` and expose async (`AsyncpgADKStore`, `AsyncmyADKStore`) or sync (`SqliteADKStore`) implementations.
- Configuration uses the `ADKConfig` TypedDict: `session_table`, `events_table`, `owner_id_column`, and `in_memory` (Oracle only).
- Call `create_tables()` once at application startup; the method is idempotent and safe to run repeatedly.
- Result records convert through `event_to_record()` / `record_to_session()` helpers, keeping Google ADK types intact.

## Installation

```bash
pip install "sqlspec[asyncpg] google-genai"
# or for SQLite development
pip install "sqlspec[aiosqlite] google-genai"
```

Choose the SQLSpec adapter that matches your production database. Recommended pairings:

- **PostgreSQL (`asyncpg`, `psycopg`, `psqlpy`)** – best JSONB support and concurrency.
- **MySQL / MariaDB (`asyncmy`)** – JSON support in 8.0+ / 10.5+.
- **SQLite (`sqlite`, `aiosqlite`)** – local development and single-user agents.
- **Oracle (`oracledb`)** – enterprise deployments with In-Memory option.

## Selecting a Store

Each adapter exposes a tailored store class:

| Adapter Package | Store Class | Notes |
| --- | --- | --- |
| `sqlspec.adapters.asyncpg.adk` | `AsyncpgADKStore` | Uses JSONB columns and `INSERT ... ON CONFLICT` |
| `sqlspec.adapters.psycopg.adk` | `PsycopgADKStore` | Works with sync/async psycopg drivers |
| `sqlspec.adapters.asyncmy.adk` | `AsyncmyADKStore` | Stores JSON as LONGTEXT with generated columns for indexes |
| `sqlspec.adapters.aiosqlite.adk` | `AiosqliteADKStore` | Async wrapper over SQLite |
| `sqlspec.adapters.sqlite.adk` | `SqliteADKStore` | Sync version for scripting contexts |
| `sqlspec.adapters.oracledb.adk` | `OracledbADKStore` | Supports `INMEMORY` option via `in_memory=True` |

All store classes inherit `BaseAsyncADKStore` or `BaseSyncADKStore`. They share method signatures (`create_session`, `append_event`, `list_sessions`, `delete_session`, etc.) so you can swap databases without changing the service layer.

## Bootstrapping the Session Service

```python
import asyncio
from google.adk.events.event import Event
from google.genai.types import Content, Part
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.adapters.asyncpg.adk import AsyncpgADKStore
from sqlspec.extensions.adk import SQLSpecSessionService


async def build_service() -> SQLSpecSessionService:
    config = AsyncpgConfig(
        pool_config={"dsn": "postgresql://localhost/agents"},
        extension_config={
            "adk": {
                "session_table": "adk_sessions",
                "events_table": "adk_events",
                "owner_id_column": "tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE",
            }
        },
    )

    store = AsyncpgADKStore(config)
    await store.create_tables()
    return SQLSpecSessionService(store)


async def run_workflow() -> None:
    service = await build_service()
    session = await service.create_session(
        app_name="weather_agent",
        user_id="user-123",
        state={"units": "metric"},
    )

    await service.append_event(
        session,
        Event(
            id="evt-1",
            author="user",
            invocation_id="inv-1",
            content=Content(parts=[Part(text="What is the forecast for tomorrow?")]),
            actions=[],
        ),
    )

    refreshed = await service.get_session(
        app_name=session.app_name,
        user_id=session.user_id,
        session_id=session.id,
    )
    print(refreshed.events[-1].content.parts[0].text)


asyncio.run(run_workflow())
```

The service automatically normalizes identifiers, timestamps, and event payloads. `append_event()` skips partial events until they complete, mirroring Google ADK semantics.

## Configuration Reference

`ADKConfig` lives in `sqlspec.extensions.adk.config` and documents the extension settings:

- `session_table` *(str)* – Session table name (default `adk_sessions`). Use snake_case ≤63 characters for PostgreSQL compatibility.
- `events_table` *(str)* – Events table name (default `adk_events`). Keep separate from session table for efficient pruning.
- `owner_id_column` *(str)* – Optional column DDL appended to both tables. SQLSpec parses the column name to populate queries and passes the definition through to DDL. Use it to enforce tenant isolation or link to users.
- `in_memory` *(bool)* – Oracle-only flag that adds the `INMEMORY` clause when creating tables. Ignored by other adapters.

Type annotate configuration for IDE help:

```python
from typing import cast
from sqlspec.extensions.adk import ADKConfig

adk_config = cast("ADKConfig", {
    "session_table": "agent_sessions",
    "events_table": "agent_events",
    "owner_id_column": "workspace_id UUID REFERENCES workspaces(id) ON DELETE SET NULL",
})
config = AsyncpgConfig(pool_config={"dsn": "postgresql://..."},
                       extension_config={"adk": adk_config})
```

## Database Operations

- **create_tables()** – Creates both tables if missing. Safe to run on every startup and during migrations.
- **create_session() / get_session() / list_sessions() / delete_session()** – Manage session lifecycle with UUID identifiers.
- **append_event() / get_events()** – Persist conversation history; supports time-based filtering via `GetSessionConfig`.
- **delete_events_before(timestamp)** – Available on adapters that expose pruning helpers (see adapter-specific docs).
- **transaction handling** – Stores run inside the owning adapter’s connection pool, so they inherit retry and transaction semantics from the driver.

## Multi-Database and Sharding Patterns

- Route different tenants to different stores by constructing multiple SQLSpec configs and mapping tenants → store instances.
- Use separate `owner_id_column` values per database to enforce constraints that match local schemas.
- Wrap store calls inside application-level transactions when combining session writes with additional domain tables.

## Monitoring and Maintenance

- Monitor insert and select latency via your database’s telemetry; stores emit structured logs with session IDs through SQLSpec’s logging system.
- Schedule periodic cleanup with adapter-provided pruning helpers or ad-hoc SQL that removes stale rows.
- Back up tables like any other transactional data; events can grow quickly, so consider partitioning or TTL policies in PostgreSQL (`CREATE POLICY ... USING (create_time > now() - interval '90 days')`).

## Additional Resources

- API reference: `docs/extensions/adk/`
- Example projects: `docs/examples/adk_basic_aiosqlite.py`, `docs/examples/adk_litestar_asyncpg.py`
- Google ADK documentation: <https://ai.google.dev/guides/adk>
