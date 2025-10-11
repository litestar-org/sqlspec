# Sanic Integration Guide

Details how to wire SQLSpec into Sanic and Sanic-Ext applications using listeners, middleware, and dependency registration.

## Quick Facts

- Install with `pip install "sqlspec[asyncpg]" sanic sanic-ext`.
- Store the `SQLSpec` instance and database configuration on `app.ctx` during startup.
- Use listeners (`before_server_start`, `before_server_stop`) to set up and tear down connection pools.
- Register dependencies through `sanic_ext.Extend` to inject sessions into handlers automatically.
- Middleware can attach sessions to `request.ctx` for manual control over commit/rollback.
- Works with both HTTP handlers and background tasks executed via `app.add_task`.

## Installation

```bash
pip install "sqlspec[asyncpg]" sanic sanic-ext
# or
uv pip install "sqlspec[asyncpg]" sanic sanic-ext
```

Sanic-Ext provides dependency injection, OpenAPI, and other niceties on top of the core Sanic framework.

## Application Setup

```python
from sanic import Sanic, Request, json
from sanic_ext import Extend
from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig

app = Sanic("sqlspec-app")
extend = Extend(app)

spec = SQLSpec()
db_config = spec.add_config(
    AsyncpgConfig(pool_config={"dsn": "postgresql://localhost/service"}),
)


@app.listener("before_server_start")
async def setup_sqlspec(app: Sanic, loop) -> None:
    app.ctx.sqlspec = spec
    app.ctx.db_config = db_config


@app.listener("before_server_stop")
async def close_sqlspec(app: Sanic, loop) -> None:
    await app.ctx.sqlspec.close_all_pools()
```

Listeners ensure pools live for the duration of the server and shut down cleanly on reload or exit.

## Dependency Injection with Sanic-Ext

```python
from sqlspec.driver import AsyncDriverAdapterBase


@extend.dependency
async def db_session(request: Request):
    async with request.app.ctx.sqlspec.provide_session(request.app.ctx.db_config) as session:
        yield session


@app.get("/users/<user_id:int>")
async def get_user(request: Request, db_session: AsyncDriverAdapterBase, user_id: int) -> object:
    result = await db_session.execute("SELECT id, email FROM users WHERE id = $1", user_id)
    return json(result.one())
```

Sanic-Ext resolves parameters by name or type annotation. The dependency function yields a SQLSpec driver, mirroring FastAPI-style dependencies.

## Manual Middleware Pattern

For fine-grained control over commit/rollback, attach middleware that stores sessions on `request.ctx`:

```python
@app.middleware("request")
async def open_session(request: Request):
    request.ctx.db_session_cm = request.app.ctx.sqlspec.provide_session(request.app.ctx.db_config)
    request.ctx.db_session = await request.ctx.db_session_cm.__aenter__()
    await request.ctx.db_session.begin()


@app.middleware("response")
async def close_session(request: Request, response):
    db_session = getattr(request.ctx, "db_session", None)
    if db_session is None:
        return
    try:
        if response.status_code < 400:
            await db_session.commit()
        else:
            await db_session.rollback()
    finally:
        await request.ctx.db_session_cm.__aexit__(None, None, None)
        request.ctx.db_session = None
        request.ctx.db_session_cm = None
```

Handlers then reuse the session via `request.ctx.db_session`. This pattern is useful when you want autocommit semantics without Sanic-Ext.

## Background Tasks and Signals

- **Background tasks** – Use `app.add_task(worker())` and open a new session inside the coroutine (`async with app.ctx.sqlspec.provide_session(...)`).
- **Signals** – Register `@app.signal("custom")` callbacks and include SQLSpec sessions as needed; listeners already hold references to the SQLSpec instance.

## Health Checks and Observability

Expose connection metrics via a simple handler:

```python
@app.get("/health")
async def health(request: Request):
    pool = request.app.ctx.sqlspec.get_pool(request.app.ctx.db_config)
    return json({"size": pool.get_size(), "free": pool.get_idle_size()})
```

Combine with Sanic-Ext’s OpenAPI generation to surface operational endpoints automatically.

## Testing Strategy

- Configure a fixture that creates a new `SQLSpec` with an in-memory database, attaches it to `app.ctx`, and tears it down after each test.
- Use Sanic’s `TestClient` to exercise routes; listeners run automatically inside the test context.
- Mock dependency outputs for isolated unit tests by overriding `extend.dependency` during setup.

## Operational Guidance

- Keep connection pool sizes aligned with Sanic worker counts; SQLSpec pools are shared across workers so avoid oversubscribing.
- Run migrations prior to starting Sanic workers to prevent race conditions.
- Monitor query latency by enabling SQLSpec logging (`sqlspec.utils.logging.get_logger("extensions.sanic")`) and forwarding logs to your observability stack.
- For multi-tenant deployments, store additional configuration (e.g., `app.ctx.analytics_config`) and extend the dependency to select the correct database based on request metadata.
