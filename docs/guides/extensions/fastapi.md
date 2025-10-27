---
orphan: true
---

# FastAPI Integration Guide

SQLSpec provides a `SQLSpecPlugin` for FastAPI that extends the Starlette integration with dependency injection helpers for FastAPI's `Depends()` system.

## Installation

```bash
pip install "sqlspec[asyncpg]" "fastapi[standard]"
# or
uv pip install "sqlspec[asyncpg]" "fastapi[standard]"
```

Replace `asyncpg` with your preferred database adapter (`psycopg`, `asyncmy`, `aiosqlite`, etc.).

## Quick Start

```python
from fastapi import Depends, FastAPI
from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.extensions.fastapi import SQLSpecPlugin

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

app = FastAPI()
db_ext = SQLSpecPlugin(sqlspec, app)


@app.get("/users")
async def list_users(db=Depends(db_ext.provide_session())):
    result = await db.execute("SELECT id, email FROM users ORDER BY id")
    return {"users": result.all()}
```

The plugin automatically:
- Creates and manages connection pools during app lifespan
- Provides request-scoped database sessions via dependency injection
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

Note: Both Starlette and FastAPI extensions use the `"starlette"` key in `extension_config`.

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

FastAPI inherits the Starlette configuration model, so you can register multiple adapters by giving each one
unique `session_key`, `connection_key`, and `pool_key` values. The plugin exposes dependency factories for each
key, while `db_ext.provide_session()` with no arguments resolves the default `"db_session"` entry.

```python
from typing import Annotated, Any

from fastapi import Depends, FastAPI
from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.extensions.fastapi import SQLSpecPlugin

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

app = FastAPI()
db_ext = SQLSpecPlugin(sqlspec, app=app)

@app.get("/reports")
async def reports(
    primary: Annotated[Any, Depends(db_ext.provide_session("primary"))],
    analytics: Annotated[Any, Depends(db_ext.provide_session("analytics"))],
    analytics_conn: Annotated[Any, Depends(db_ext.provide_connection("analytics"))],
) -> dict[str, int]:
    total_users = await primary.select_value("SELECT COUNT(*) FROM users")
    await analytics.execute("INSERT INTO page_views(path) VALUES (:path)", {"path": "/reports"})
    await analytics_conn.commit()
    return {"users": total_users}
```

### Key Requirements

- Each configuration must define unique `session_key`, `connection_key`, and `pool_key` entries under the
  `"starlette"` extension config.
- Use the same string when wiring dependencies via `db_ext.provide_session("key")` or
  `db_ext.provide_connection("key")`.
- The default dependency `db_ext.provide_session()` resolves the implicit `"db_session"` configuration if you only
  register one database.

### Troubleshooting

- **"Duplicate state keys found"** → Duplicate `session_key`, `connection_key`, or `pool_key`. Assign unique values.
- **"No configuration found for key"** → The dependency references a key that has not been configured.
- **Unexpected connection state** → Remember to await commits/rollbacks on the connection dependency when using
  manual commit mode.


## Dependency Injection

The FastAPI plugin provides two dependency factories:

### Session Dependency

Inject database sessions (SQLSpec drivers) into route handlers:

```python
@app.get("/users/{user_id}")
async def get_user(
    user_id: int,
    db=Depends(db_ext.provide_session())
):
    result = await db.execute(
        "SELECT id, email FROM users WHERE id = $1",
        user_id
    )
    return result.one()
```

The session provides the full SQLSpec driver API: `execute()`, `select_one()`, `select_all()`, etc.

### Connection Dependency

Inject raw database connections for driver-specific operations:

```python
@app.get("/raw")
async def raw_query(conn=Depends(db_ext.provide_connection())):
    cursor = await conn.cursor()
    await cursor.execute("SELECT 1")
    result = await cursor.fetchone()
    return {"result": result}
```

Use connection dependencies when you need direct access to the underlying database driver.

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


@app.post("/users", status_code=201)
async def create_user(
    email: str,
    db=Depends(db_ext.provide_session()),
    conn=Depends(db_ext.provide_connection())
):
    await db.execute(
        "INSERT INTO users (email) VALUES ($1)",
        email
    )

    await conn.commit()

    return {"created": True}
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


@app.post("/users", status_code=201)
async def create_user(
    email: str,
    db=Depends(db_ext.provide_session())
):
    await db.execute(
        "INSERT INTO users (email) VALUES ($1)",
        email
    )

    return {"created": True}
```

Automatically commits because status is 201 (2xx range). No explicit commit needed.

### Autocommit with Redirects

Commits on 2xx and 3xx status codes:

```python
from fastapi.responses import RedirectResponse

config = AsyncpgConfig(
    pool_config={"dsn": "postgresql://localhost/mydb"},
    extension_config={"starlette": {"commit_mode": "autocommit_include_redirect"}}
)

sqlspec.add_config(config)
db_ext = SQLSpecPlugin(sqlspec, app)


@app.post("/users")
async def create_user(
    email: str,
    db=Depends(db_ext.provide_session())
):
    await db.execute(
        "INSERT INTO users (email) VALUES ($1)",
        email
    )

    return RedirectResponse(url="/users", status_code=303)
```

Automatically commits because status is 303 (3xx range).

## Multi-Database Configuration

Configure multiple databases and inject them via separate dependencies:

```python
from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.adapters.asyncmy import AsyncmyConfig
from sqlspec.extensions.fastapi import SQLSpecPlugin

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

app = FastAPI()
db_ext = SQLSpecPlugin(sqlspec, app)


@app.get("/dashboard")
async def dashboard(
    pg_db=Depends(db_ext.provide_session("pg_db")),
    mysql_db=Depends(db_ext.provide_session("mysql_db"))
):
    users = await pg_db.execute("SELECT COUNT(*) FROM users")
    events = await mysql_db.execute("SELECT COUNT(*) FROM events")

    return {
        "users": users.scalar(),
        "events": events.scalar()
    }
```

Each database maintains its own pool, session cache, and transaction handling.

## Pydantic Integration

Use Pydantic models for type-safe request/response handling:

```python
from pydantic import BaseModel, EmailStr


class UserCreate(BaseModel):
    email: EmailStr


class User(BaseModel):
    id: int
    email: str


@app.post("/users", response_model=User, status_code=201)
async def create_user(
    user: UserCreate,
    db=Depends(db_ext.provide_session())
):
    result = await db.execute(
        "INSERT INTO users (email) VALUES ($1) RETURNING id, email",
        user.email
    )
    row = result.one()
    return User(id=row["id"], email=row["email"])


@app.get("/users/{user_id}", response_model=User)
async def get_user(
    user_id: int,
    db=Depends(db_ext.provide_session())
):
    result = await db.execute(
        "SELECT id, email FROM users WHERE id = $1",
        user_id
    )
    row = result.one()
    return User(**row)
```

Pydantic models provide validation, serialization, and API documentation.

## Typed Results with Msgspec

Map query results directly to msgspec structs:

```python
import msgspec
from fastapi import Depends


class User(msgspec.Struct):
    id: int
    email: str


@app.get("/users")
async def list_users(db=Depends(db_ext.provide_session())) -> list[User]:
    result = await db.execute("SELECT id, email FROM users ORDER BY id")
    return result.all(schema_type=User)


@app.get("/users/{user_id}")
async def get_user(
    user_id: int,
    db=Depends(db_ext.provide_session())
) -> User:
    result = await db.execute(
        "SELECT id, email FROM users WHERE id = $1",
        user_id
    )
    return result.get_first(schema_type=User)
```

Msgspec provides fast serialization and automatic FastAPI response conversion.

## Session Caching

Sessions are cached per request to ensure consistency:

```python
@app.get("/example")
async def example(
    db1=Depends(db_ext.provide_session()),
    db2=Depends(db_ext.provide_session())
):
    assert db1 is db2

    await db1.execute(
        "INSERT INTO users (email) VALUES ($1)",
        "test@example.com"
    )

    result = await db2.execute(
        "SELECT * FROM users WHERE email = $1",
        "test@example.com"
    )

    return {"user": result.get_first()}
```

Both dependencies return the same session object, ensuring transactional consistency.

## Lifecycle Management

The plugin automatically manages pool lifecycle when initialized with the app:

```python
app = FastAPI()
db_ext = SQLSpecPlugin(sqlspec, app)
```

If you need manual control over lifecycle:

```python
from contextlib import asynccontextmanager


db_ext = SQLSpecPlugin(sqlspec)


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with db_ext.lifespan(app):
        yield


app = FastAPI(lifespan=lifespan)
db_ext.init_app(app)
```

The plugin integrates seamlessly with existing lifespan handlers.

## Testing

Use `TestClient` with dependency overrides for testing:

```python
from fastapi.testclient import TestClient
from sqlspec.adapters.aiosqlite import AiosqliteConfig


def test_users_endpoint():
    sqlspec = SQLSpec()
    config = AiosqliteConfig(
        pool_config={"database": ":memory:"},
        extension_config={"starlette": {"commit_mode": "autocommit"}}
    )
    sqlspec.add_config(config)

    app = FastAPI()
    db_ext = SQLSpecPlugin(sqlspec, app)

    @app.post("/users", status_code=201)
    async def create_user(
        email: str,
        db=Depends(db_ext.provide_session())
    ):
        await db.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, email TEXT)")
        await db.execute("INSERT INTO users (email) VALUES (?)", (email,))
        return {"created": True}

    @app.get("/users")
    async def list_users(db=Depends(db_ext.provide_session())):
        result = await db.execute("SELECT * FROM users")
        return {"users": result.all()}

    db_ext.init_app(app)

    with TestClient(app) as client:
        response = client.post("/users?email=test@example.com")
        assert response.status_code == 201

        response = client.get("/users")
        assert response.status_code == 200
        assert len(response.json()["users"]) == 1
```

## Background Tasks

Sessions are request-scoped and should not be passed to background tasks. Create new sessions within background tasks:

```python
from fastapi import BackgroundTasks


async def send_email(email: str, config, sqlspec):
    async with sqlspec.provide_session(config) as db:
        await db.execute(
            "INSERT INTO email_log (email, sent_at) VALUES ($1, NOW())",
            email
        )


@app.post("/signup", status_code=201)
async def signup(
    email: str,
    background_tasks: BackgroundTasks,
    db=Depends(db_ext.provide_session())
):
    await db.execute(
        "INSERT INTO users (email) VALUES ($1)",
        email
    )

    background_tasks.add_task(send_email, email, config, sqlspec)

    return {"created": True}
```

Background tasks use `sqlspec.provide_session()` to create independent sessions.

## WebSocket Support

WebSocket connections create their own sessions:

```python
from fastapi import WebSocket


@app.websocket("/ws")
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
from fastapi import HTTPException


@app.post("/users", status_code=201)
async def create_user(
    email: str,
    db=Depends(db_ext.provide_session())
):
    await db.execute(
        "INSERT INTO users (email) VALUES ($1)",
        email
    )

    if "@" not in email:
        raise HTTPException(status_code=400, detail="Invalid email")

    return {"created": True}
```

If the `HTTPException` is raised, the middleware automatically rolls back the INSERT.

## Nested Dependencies

Combine database dependencies with other FastAPI dependencies:

```python
from fastapi import Header


async def get_current_user(
    authorization: str = Header(...),
    db=Depends(db_ext.provide_session())
):
    token = authorization.replace("Bearer ", "")
    result = await db.execute(
        "SELECT * FROM users WHERE token = $1",
        token
    )
    return result.one()


@app.get("/profile")
async def get_profile(user=Depends(get_current_user)):
    return {"user": user}


@app.post("/posts", status_code=201)
async def create_post(
    content: str,
    user=Depends(get_current_user),
    db=Depends(db_ext.provide_session())
):
    await db.execute(
        "INSERT INTO posts (user_id, content) VALUES ($1, $2)",
        user["id"],
        content
    )
    return {"created": True}
```

Database sessions are shared across nested dependencies within the same request.

## Best Practices

1. **Use autocommit mode for simple APIs**: Reduces boilerplate and ensures consistency
2. **Use manual mode for complex transactions**: Provides fine-grained control when needed
3. **Leverage dependency injection**: Keep route handlers focused on business logic
4. **Use Pydantic/msgspec for type safety**: Automatic validation and serialization
5. **Separate background task sessions**: Don't pass request sessions to background tasks
6. **Configure unique keys for multiple databases**: Prevents state key collisions
7. **Test with in-memory databases**: Faster tests with SQLite `:memory:` databases

## Migration to Plugin

If you're using the old manual pattern, migration is straightforward:

**Before (manual pattern)**:

```python
from collections.abc import AsyncGenerator

spec = SQLSpec()
db_config = spec.add_config(AsyncpgConfig(pool_config={"dsn": "postgresql://..."}))


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await spec.close_all_pools()


app = FastAPI(lifespan=lifespan)


async def get_session() -> AsyncGenerator[AsyncDriverAdapterBase, None]:
    async with spec.provide_session(db_config) as session:
        yield session


@app.post("/users", status_code=201)
async def create_user(
    payload: UserIn,
    db: AsyncDriverAdapterBase = Depends(get_session),
):
    await db.begin()
    try:
        result = await db.execute("INSERT INTO users (email) VALUES ($1) RETURNING id", payload.email)
    except Exception:
        await db.rollback()
        raise
    else:
        await db.commit()
        return {"id": result.scalar(), "email": payload.email}
```

**After (plugin pattern)**:

```python
sqlspec = SQLSpec()
config = AsyncpgConfig(
    pool_config={"dsn": "postgresql://..."},
    extension_config={"starlette": {"commit_mode": "autocommit"}}
)
sqlspec.add_config(config)

app = FastAPI()
db_ext = SQLSpecPlugin(sqlspec, app)


@app.post("/users", status_code=201)
async def create_user(
    payload: UserIn,
    db=Depends(db_ext.provide_session())
):
    result = await db.execute(
        "INSERT INTO users (email) VALUES ($1) RETURNING id",
        payload.email
    )
    return {"id": result.scalar(), "email": payload.email}
```

Benefits:
- Automatic pool lifecycle management
- Automatic transaction handling
- Session caching built-in
- Multi-database support
- Less boilerplate code
- Type-safe dependency injection

## Relationship to Starlette

The FastAPI `SQLSpecPlugin` inherits from the Starlette plugin and adds dependency injection helpers. All Starlette features work in FastAPI:

- Pool lifecycle management
- Middleware-based transactions
- Session caching
- Multi-database support

The key difference is FastAPI's `Depends()` system provides cleaner dependency injection than manually calling `db_ext.get_session(request)`.
