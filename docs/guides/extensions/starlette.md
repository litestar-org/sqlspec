---
orphan: true
---

# Starlette Integration Guide

Outlines patterns for running SQLSpec inside Starlette applications using lifespan management, request dependencies, and middleware-driven transaction control.

## Quick Facts

- Install with `pip install "sqlspec[asyncpg]" starlette` (swap the adapter extra for your database).
- Use Starlette’s lifespan context to initialize `SQLSpec` and close pools on shutdown.
- Store configurations on `app.state` so routes and middleware can access them without globals.
- Inject sessions through lightweight dependency callables that wrap `spec.provide_session(config)`.
- Wrap database operations in `BaseHTTPMiddleware` when you need automatic commit/rollback.
- Reuse the same dependency helpers in background tasks and WebSocket endpoints.

## Installation

```bash
pip install "sqlspec[asyncpg]" starlette
# or
uv pip install "sqlspec[asyncpg]" starlette
```

Starlette does not require a dedicated SQLSpec extra. Install the framework alongside the database adapter you intend to use.

## Application Setup

```python
from contextlib import asynccontextmanager
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route
from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.driver import AsyncDriverAdapterBase

spec = SQLSpec()
config = spec.add_config(
    AsyncpgConfig(pool_config={"dsn": "postgresql://localhost/app"}),
)


@asynccontextmanager
async def lifespan(app: Starlette):
    app.state.sqlspec = spec
    app.state.db_config = config
    yield
    await spec.close_all_pools()


async def list_users(request) -> JSONResponse:
    async with request.app.state.sqlspec.provide_session(request.app.state.db_config) as session:
        result = await session.execute("SELECT id, email FROM users ORDER BY id")
        return JSONResponse({"users": result.all()})


app = Starlette(routes=[Route("/users", list_users)], lifespan=lifespan)
```

The lifespan handler opens access to the `SQLSpec` instance and ensures pools shut down cleanly during application exit or reload.

## Dependency Helpers

Factor out a reusable dependency that yields a session per request:

```python
from contextlib import asynccontextmanager
from starlette.requests import Request


@asynccontextmanager
async def session_scope(request: Request):
    session_cm = request.app.state.sqlspec.provide_session(request.app.state.db_config)
    session = await session_cm.__aenter__()
    try:
        yield session
    finally:
        await session_cm.__aexit__(None, None, None)
```

Use the dependency inside route callables:

```python
async def create_user(request: Request) -> JSONResponse:
    data = await request.json()
    async with session_scope(request) as session:
        await session.begin()
        try:
            result = await session.execute(
                "INSERT INTO users (email) VALUES ($1) RETURNING id",
                data["email"],
            )
        except Exception:
            await session.rollback()
            raise
        else:
            await session.commit()
            return JSONResponse({"id": result.scalar()}, status_code=201)
```

The explicit `async for` keeps the dependency self-contained without relying on FastAPI-style dependency injection.

## Automatic Transaction Middleware

Autocommit behavior similar to the Litestar plugin can be reproduced with middleware:

```python
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response


class TransactionMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        async with request.app.state.sqlspec.provide_session(request.app.state.db_config) as session:
            request.state.db_session = session
            await session.begin()
            try:
                response = await call_next(request)
            except Exception:
                await session.rollback()
                raise
            else:
                if 200 <= response.status_code < 300:
                    await session.commit()
                else:
                    await session.rollback()
                return response
            finally:
                request.state.db_session = None
```

Attach the middleware and reuse the stored session:

```python
app.add_middleware(TransactionMiddleware)


async def delete_user(request: Request) -> Response:
    session: AsyncDriverAdapterBase = request.state.db_session
    await session.execute("DELETE FROM users WHERE id = $1", int(request.path_params["user_id"]))
    return Response(status_code=204)
```

## WebSockets and Background Tasks

- **WebSockets** – Acquire sessions inside the handler using `provide_connection` or `provide_session` and close them in `finally` blocks.
- **Background tasks** – Pass the session or the SQLSpec config into the task and call `spec.provide_session(config)` inside the task to maintain isolation from request-scoped connections.

## Testing Patterns

- Use `TestClient` with the same lifespan context; override the DSN to point at a test database.
- For async tests, instantiate `spec` with an in-memory adapter (e.g., `AiosqliteConfig(database=":memory:")`) and wrap tests with `pytest.mark.asyncio`.
- Seed data using `async with spec.provide_session(config)` within fixtures so cleanup runs automatically.

## Operational Tips

- Centralize SQLSpec configuration definitions to avoid duplicating connection URLs across middleware and dependencies.
- Log request IDs alongside database queries by adding logging middleware before the transaction middleware.
- Gracefully handle driver exceptions in middleware to ensure rollbacks fire even when `call_next` raises.
- Periodically run database migrations outside the Starlette process; SQLSpec maintains compatibility with Alembic-style workflows through its migration helpers.
