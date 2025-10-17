---
orphan: true
---

# FastAPI Integration Guide

Shows how to integrate SQLSpec with FastAPI using dependency injection, lifespan management, and transaction control for async APIs.

## Quick Facts

- Install with `pip install "sqlspec[asyncpg]" "fastapi[standard]"` for full ASGI support (or `uv pip ...`).
- Manage `SQLSpec` inside a lifespan context manager to open pools on startup and close them on shutdown.
- Dependency functions wrap `spec.provide_session(config)` and return drivers typed as `AsyncDriverAdapterBase`.
- Explicit transactions call `await session.begin()` / `await session.commit()`; simple CRUD works without manual scope.
- Multiple databases map cleanly to separate dependency callables, each referencing a distinct configuration.
- Background tasks and WebSockets reuse the same dependency helpers.

## Installation

```bash
pip install "sqlspec[asyncpg]" "fastapi[standard]"
# or
uv pip install "sqlspec[asyncpg]" "fastapi[standard]"
```

Swap the adapter extra for your target database (`[psycopg]`, `[asyncmy]`, `[aiosqlite]`, etc.).

## Application Setup

```python
from contextlib import asynccontextmanager
from fastapi import Depends, FastAPI
from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.driver import AsyncDriverAdapterBase

spec = SQLSpec()
db_config = spec.add_config(
    AsyncpgConfig(pool_config={"dsn": "postgresql://localhost/app"}),
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await spec.close_all_pools()


app = FastAPI(lifespan=lifespan)


from collections.abc import AsyncGenerator


async def get_session() -> AsyncGenerator[AsyncDriverAdapterBase, None]:
    async with spec.provide_session(db_config) as session:
        yield session
```

The dependency yields a driver instance that handles parameter conversion, caching, and result typing.

## CRUD Handlers

```python
from fastapi import status
from pydantic import BaseModel


class UserIn(BaseModel):
    email: str


@app.post("/users", status_code=status.HTTP_201_CREATED)
async def create_user(
    payload: UserIn,
    db: AsyncDriverAdapterBase = Depends(get_session),
) -> dict[str, object]:
    await db.begin()
    try:
        result = await db.execute(
            "INSERT INTO users (email) VALUES ($1) RETURNING id",
            payload.email,
        )
    except Exception:
        await db.rollback()
        raise
    else:
        await db.commit()
        return {"id": result.scalar(), "email": payload.email}


@app.get("/users/{user_id}")
async def read_user(
    user_id: int,
    db: AsyncDriverAdapterBase = Depends(get_session),
) -> dict[str, object]:
    result = await db.execute("SELECT id, email FROM users WHERE id = $1", user_id)
    return result.one()
```

Use transactions for multi-statement operations; FastAPI handles dependency cleanup even when exceptions propagate.

## Multiple Databases

```python
analytics_config = spec.add_config(
    AsyncpgConfig(pool_config={"dsn": "postgresql://localhost/analytics"}),
)


async def get_main_db() -> AsyncGenerator[AsyncDriverAdapterBase, None]:
    async with spec.provide_session(db_config) as session:
        yield session


async def get_analytics_db() -> AsyncGenerator[AsyncDriverAdapterBase, None]:
    async with spec.provide_session(analytics_config) as session:
        yield session


@app.get("/dashboard")
async def dashboard(
    main_db: AsyncDriverAdapterBase = Depends(get_main_db),
    analytics_db: AsyncDriverAdapterBase = Depends(get_analytics_db),
) -> dict[str, int]:
    users = await main_db.execute("SELECT COUNT(*) FROM users")
    events = await analytics_db.execute("SELECT COUNT(*) FROM events")
    return {"users": users.scalar(), "events": events.scalar()}
```

Each dependency yields an independent session scoped to the downstream handler.

## Transactions and Error Handling

- Call `await db.begin()` before multi-statement workflows and follow with `await db.commit()` or `await db.rollback()` depending on the outcome.
- For simple read-only endpoints, skip the explicit transaction and rely on implicit autocommit semantics.
- Propagate exceptions; FastAPI will return error responses and SQLSpec ensures sessions are closed.
- Attach middleware when you need automatic commit on specific status ranges (e.g., wrap dependency logic in custom middleware similar to the Starlette example).

## Background Jobs and WebSockets

- Background tasks defined with `BackgroundTasks` should open their own session inside the task body (`async with spec.provide_session(...)`).
- WebSocket endpoints can reuse the dependency pattern by calling `await get_session().__anext__()` within the handler and closing it in `finally`.

## Testing Strategy

- Override dependencies with fixtures that return in-memory adapters (e.g., SQLite) using FastAPI’s `app.dependency_overrides`.
- Use `pytest` with `asyncio` support and call `spec.close_all_pools()` during teardown to eliminate dangling connections.
- Seed the database via fixtures that leverage SQLSpec sessions instead of raw driver connections to keep parameter conversion consistent.

## Operational Recommendations

- Manage migrations with your framework of choice (Alembic, `sqlspec.migrations`). FastAPI’s startup events are ideal for invoking migration runners.
- Instrument SQLSpec’s logging (`sqlspec.utils.logging.get_logger`) to emit structured logs; align request IDs using FastAPI middleware.
- Keep connection pool sizes in sync with Uvicorn workers to avoid oversubscription.
- Monitor connection usage via the adapter-specific pool object (`spec.get_pool(config)`), exposed for frameworks that need runtime diagnostics.
