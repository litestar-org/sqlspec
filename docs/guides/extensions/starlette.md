---
orphan: true
---

# Starlette Integration Guide

SQLSpec provides a `SQLSpecPlugin` for Starlette that handles connection pooling, request-scoped sessions, and automatic transaction management through middleware.

## Installation

```bash
pip install "sqlspec[asyncpg]" starlette
# or
uv pip install "sqlspec[asyncpg]" starlette
```

Replace `asyncpg` with your preferred database adapter (`psycopg`, `asyncmy`, `aiosqlite`, etc.).

## Quick Start

```python
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route
from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.extensions.starlette import SQLSpecPlugin

sqlspec = SQLSpec()
config = AsyncpgConfig(
    pool_config={"dsn": "postgresql://localhost/mydb"},
    extension_config={
        "starlette": {
            "commit_mode": "autocommit",
            "session_key": "db"
        }
    }
)
sqlspec.add_config(config, name="default")

app = Starlette()
db_ext = SQLSpecPlugin(sqlspec, app)


@app.route("/users")
async def list_users(request):
    db = db_ext.get_session(request)
    result = await db.execute("SELECT id, email FROM users ORDER BY id")
    return JSONResponse({"users": result.all()})
```

The plugin automatically:
- Creates and manages connection pools during app lifespan
- Provides request-scoped database sessions
- Handles transaction commit/rollback based on response status
- Caches sessions per request for consistency

## Configuration

Configure the plugin via `extension_config["starlette"]` in your database config:

```python
from sqlspec.config import StarletteConfig

config = AsyncpgConfig(
    pool_config={"dsn": "postgresql://localhost/mydb"},
    extension_config={
        "starlette": StarletteConfig(
            commit_mode="autocommit",
            session_key="db",
            connection_key="db_connection",
            pool_key="db_pool",
            extra_commit_statuses={201, 202},
            extra_rollback_statuses={409}
        )
    }
)
```

### Configuration Options

- **commit_mode**: Transaction handling mode (default: `"manual"`)
  - `"manual"`: No automatic transactions
  - `"autocommit"`: Commit on 2xx status, rollback otherwise
  - `"autocommit_include_redirect"`: Commit on 2xx-3xx status, rollback otherwise
- **session_key**: Key for storing session in `request.state` (default: `"db_session"`)
- **connection_key**: Key for storing connection in `request.state` (default: `"db_connection"`)
- **pool_key**: Key for storing pool in `app.state` (default: `"db_pool"`)
- **extra_commit_statuses**: Additional HTTP statuses that trigger commit (default: empty set)
- **extra_rollback_statuses**: Additional HTTP statuses that trigger rollback (default: empty set)

## Multi-Database Configuration

Starlette supports running multiple adapters side by side. Give each configuration its own
`session_key`, `connection_key`, and `pool_key`, then resolve the appropriate session inside each request.
`db_ext.get_session(request)` without a key uses the default `"db_session"` entry.

```python
from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.extensions.starlette import SQLSpecPlugin
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route

sqlspec = SQLSpec()
sqlspec.add_config(
    AsyncpgConfig(
        pool_config={"dsn": "postgresql://localhost/main"},
        extension_config={
            "starlette": {
                "session_key": "primary",
                "connection_key": "primary_connection",
                "pool_key": "primary_pool",
                "commit_mode": "autocommit",
            }
        },
    )
)

sqlspec.add_config(
    AiosqliteConfig(
        pool_config={"database": "analytics.db"},
        extension_config={
            "starlette": {
                "session_key": "analytics",
                "connection_key": "analytics_connection",
                "pool_key": "analytics_pool",
                "commit_mode": "manual",
            }
        },
    )
)

db_ext = SQLSpecPlugin(sqlspec)

async def report(request: Request) -> JSONResponse:
    core = db_ext.get_session(request, "primary")
    analytics = db_ext.get_session(request, "analytics")
    total_users = await core.select_value("SELECT COUNT(*) FROM users")
    await analytics.execute("INSERT INTO page_views(path) VALUES (:path)", {"path": request.url.path})
    analytics_conn = db_ext.get_connection(request, "analytics")
    await analytics_conn.commit()
    return JSONResponse({"users": total_users})

app = Starlette(routes=[Route("/reports", report)])
db_ext.init_app(app)
```

### Key Requirements

- `session_key`, `connection_key`, and `pool_key` must be unique per configuration so the middleware can cache
  values without collisions.
- The first registered configuration becomes the implicit default for `db_ext.get_session(request)`.
- Keep `connection_key` and `pool_key` readable—deriving them from `session_key` (e.g., `f"{session_key}_connection"`) helps avoid typos.

### Troubleshooting

- **"Duplicate state keys found"** → One of the keys overlaps between configs. Ensure all three keys are unique.
- **"No configuration found for key"** → The requested `session_key` is missing or misspelled.
- **Pool not closing** → The plugin closes pools inside the application lifespan. Ensure your ASGI server triggers
  startup/shutdown (e.g., use `with TestClient(app)` during tests).

## Commit Modes

### Manual Mode

Requires explicit transaction management:

```python
config = AsyncpgConfig(
    pool_config={"dsn": "postgresql://localhost/mydb"},
    extension_config={"starlette": {"commit_mode": "manual"}}
)

sqlspec.add_config(config)
db_ext = SQLSpecPlugin(sqlspec, app)


@app.route("/users", methods=["POST"])
async def create_user(request):
    db = db_ext.get_session(request)
    data = await request.json()

    await db.execute(
        "INSERT INTO users (email) VALUES ($1)",
        data["email"]
    )

    conn = db_ext.get_connection(request)
    await conn.commit()

    return JSONResponse({"created": True}, status_code=201)
```

Use manual mode when you need fine-grained control over transaction boundaries.

### Autocommit Mode

Automatically commits on 2xx status codes, rolls back otherwise:

```python
config = AsyncpgConfig(
    pool_config={"dsn": "postgresql://localhost/mydb"},
    extension_config={"starlette": {"commit_mode": "autocommit"}}
)

sqlspec.add_config(config)
db_ext = SQLSpecPlugin(sqlspec, app)


@app.route("/users", methods=["POST"])
async def create_user(request):
    db = db_ext.get_session(request)
    data = await request.json()

    await db.execute(
        "INSERT INTO users (email) VALUES ($1)",
        data["email"]
    )

    return JSONResponse({"created": True}, status_code=201)
```

Automatically commits because status is 201 (2xx range).

### Autocommit with Redirects

Commits on 2xx and 3xx status codes:

```python
config = AsyncpgConfig(
    pool_config={"dsn": "postgresql://localhost/mydb"},
    extension_config={"starlette": {"commit_mode": "autocommit_include_redirect"}}
)

sqlspec.add_config(config)
db_ext = SQLSpecPlugin(sqlspec, app)


@app.route("/users", methods=["POST"])
async def create_user(request):
    db = db_ext.get_session(request)
    data = await request.json()

    await db.execute(
        "INSERT INTO users (email) VALUES ($1)",
        data["email"]
    )

    return RedirectResponse(url="/users", status_code=303)
```

Automatically commits because status is 303 (3xx range).

## Multi-Database Configuration

Configure multiple databases and access them by key:

```python
from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.adapters.asyncmy import AsyncmyConfig
from sqlspec.extensions.starlette import SQLSpecPlugin

sqlspec = SQLSpec()

pg_config = AsyncpgConfig(
    pool_config={"dsn": "postgresql://localhost/main"},
    extension_config={
        "starlette": {
            "commit_mode": "autocommit",
            "session_key": "pg_db"
        }
    }
)

mysql_config = AsyncmyConfig(
    pool_config={"dsn": "mysql://localhost/analytics"},
    extension_config={
        "starlette": {
            "commit_mode": "autocommit",
            "session_key": "mysql_db"
        }
    }
)

sqlspec.add_config(pg_config, name="postgres")
sqlspec.add_config(mysql_config, name="mysql")

app = Starlette()
db_ext = SQLSpecPlugin(sqlspec, app)


@app.route("/dashboard")
async def dashboard(request):
    pg_db = db_ext.get_session(request, key="pg_db")
    mysql_db = db_ext.get_session(request, key="mysql_db")

    users = await pg_db.execute("SELECT COUNT(*) FROM users")
    events = await mysql_db.execute("SELECT COUNT(*) FROM events")

    return JSONResponse({
        "users": users.scalar(),
        "events": events.scalar()
    })
```

Each database maintains its own pool, session cache, and transaction handling.

## Session Caching

Sessions are cached per request to ensure consistency:

```python
@app.route("/example")
async def example(request):
    db1 = db_ext.get_session(request)
    db2 = db_ext.get_session(request)

    assert db1 is db2

    await db1.execute("INSERT INTO users (email) VALUES ($1)", "test@example.com")

    result = await db2.execute("SELECT * FROM users WHERE email = $1", "test@example.com")

    return JSONResponse({"user": result.get_first()})
```

Both `db1` and `db2` reference the same session object, ensuring transactional consistency.

## Connection Access

Access raw database connections when needed:

```python
@app.route("/raw")
async def raw_query(request):
    conn = db_ext.get_connection(request)

    cursor = await conn.cursor()
    await cursor.execute("SELECT 1")
    result = await cursor.fetchone()

    return JSONResponse({"result": result})
```

Use `get_connection()` for driver-specific operations not exposed by the SQLSpec session API.

## Lifecycle Management

The plugin automatically manages pool lifecycle when initialized with the app:

```python
app = Starlette()
db_ext = SQLSpecPlugin(sqlspec, app)
```

If you need manual control over lifecycle:

```python
from contextlib import asynccontextmanager


db_ext = SQLSpecPlugin(sqlspec)


@asynccontextmanager
async def lifespan(app: Starlette):
    async with db_ext.lifespan(app):
        yield


app = Starlette(lifespan=lifespan)
db_ext.init_app(app)
```

The plugin integrates seamlessly with existing lifespan handlers.

## Testing

Use `TestClient` with in-memory databases for testing:

```python
from starlette.testclient import TestClient
from sqlspec.adapters.aiosqlite import AiosqliteConfig


def test_users_endpoint():
    sqlspec = SQLSpec()
    config = AiosqliteConfig(
        pool_config={"database": ":memory:"},
        extension_config={"starlette": {"commit_mode": "autocommit"}}
    )
    sqlspec.add_config(config)

    app = Starlette()
    db_ext = SQLSpecPlugin(sqlspec, app)

    @app.route("/users")
    async def list_users(request):
        db = db_ext.get_session(request)
        await db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)")
        await db.execute("INSERT INTO users (email) VALUES (?)", ("test@example.com",))
        result = await db.execute("SELECT * FROM users")
        return JSONResponse({"users": result.all()})

    db_ext.init_app(app)

    with TestClient(app) as client:
        response = client.get("/users")
        assert response.status_code == 200
        assert len(response.json()["users"]) == 1
```

## Background Tasks

Sessions are request-scoped and should not be passed to background tasks. Create new sessions within background tasks:

```python
from starlette.background import BackgroundTask


async def send_email(email: str, config, sqlspec):
    async with sqlspec.provide_session(config) as db:
        await db.execute(
            "INSERT INTO email_log (email, sent_at) VALUES ($1, NOW())",
            email
        )


@app.route("/signup", methods=["POST"])
async def signup(request):
    data = await request.json()
    db = db_ext.get_session(request)

    await db.execute(
        "INSERT INTO users (email) VALUES ($1)",
        data["email"]
    )

    task = BackgroundTask(
        send_email,
        email=data["email"],
        config=config,
        sqlspec=sqlspec
    )

    return JSONResponse({"created": True}, status_code=201, background=task)
```

Background tasks use `sqlspec.provide_session()` to create independent sessions.

## WebSocket Support

WebSocket connections create their own sessions:

```python
from starlette.websockets import WebSocket


@app.websocket_route("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()

    async with sqlspec.provide_session(config) as db:
        while True:
            data = await websocket.receive_text()
            result = await db.execute(
                "SELECT * FROM messages WHERE content LIKE $1",
                f"%{data}%"
            )
            await websocket.send_json({"messages": result.all()})
```

WebSocket handlers use `provide_session()` directly since they don't have HTTP request/response cycles.

## Error Handling

The plugin automatically rolls back transactions on exceptions in autocommit mode:

```python
@app.route("/users", methods=["POST"])
async def create_user(request):
    db = db_ext.get_session(request)
    data = await request.json()

    await db.execute(
        "INSERT INTO users (email) VALUES ($1)",
        data["email"]
    )

    if not data.get("verified"):
        raise ValueError("Email must be verified")

    return JSONResponse({"created": True}, status_code=201)
```

If the `ValueError` is raised, the middleware automatically rolls back the INSERT.

## Best Practices

1. **Use autocommit mode for simple APIs**: Reduces boilerplate and ensures consistency
2. **Use manual mode for complex transactions**: Provides fine-grained control when needed
3. **Cache sessions per request**: Always use `get_session()` instead of creating new sessions
4. **Separate background task sessions**: Don't pass request sessions to background tasks
5. **Configure unique keys for multiple databases**: Prevents state key collisions
6. **Test with in-memory databases**: Faster tests with SQLite `:memory:` databases

## Migration to Plugin

If you're using the old manual pattern, migration is straightforward:

**Before (manual pattern)**:

```python
spec = SQLSpec()
config = spec.add_config(AsyncpgConfig(pool_config={"dsn": "postgresql://..."}))


@asynccontextmanager
async def lifespan(app: Starlette):
    app.state.sqlspec = spec
    app.state.db_config = config
    yield
    await spec.close_all_pools()


async def list_users(request) -> JSONResponse:
    async with request.app.state.sqlspec.provide_session(request.app.state.db_config) as session:
        result = await session.execute("SELECT * FROM users")
        return JSONResponse({"users": result.all()})


app = Starlette(routes=[Route("/users", list_users)], lifespan=lifespan)
```

**After (plugin pattern)**:

```python
sqlspec = SQLSpec()
config = AsyncpgConfig(
    pool_config={"dsn": "postgresql://..."},
    extension_config={"starlette": {"commit_mode": "autocommit"}}
)
sqlspec.add_config(config)

app = Starlette()
db_ext = SQLSpecPlugin(sqlspec, app)


@app.route("/users")
async def list_users(request):
    db = db_ext.get_session(request)
    result = await db.execute("SELECT * FROM users")
    return JSONResponse({"users": result.all()})
```

Benefits:
- Automatic pool lifecycle management
- Automatic transaction handling
- Session caching built-in
- Multi-database support
- Less boilerplate code
