# Framework Integration Patterns

Guide to integrating SQLSpec with web frameworks.

## Litestar (Gold Standard)

```python
from litestar import Litestar, get
from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgDriver
from sqlspec.extensions.litestar import SQLSpecPlugin

spec = SQLSpec()
db = spec.add_config(
    AsyncpgConfig(
        connection_config={"dsn": "postgresql://localhost/db"},
        extension_config={
            "litestar": {
                "commit_mode": "autocommit",
                "session_key": "db_session",
                "enable_correlation_middleware": True,
            }
        }
    )
)

app = Litestar(
    route_handlers=[...],
    plugins=[SQLSpecPlugin(sqlspec=spec)]
)

# Dependency injection via type annotation
@get("/users/{user_id:int}")
async def get_user(user_id: int, db_session: AsyncpgDriver) -> dict:
    return await db_session.select_one(
        "SELECT * FROM users WHERE id = $1", user_id
    )
```

## FastAPI

```python
from fastapi import FastAPI, Depends
from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgDriver
from sqlspec.extensions.fastapi import SQLSpecPlugin

spec = SQLSpec()
db = spec.add_config(AsyncpgConfig(connection_config={...}))
plugin = SQLSpecPlugin(spec)

app = FastAPI()
plugin.init_app(app)

@app.get("/users/{user_id}")
async def get_user(
    user_id: int,
    db: AsyncpgDriver = Depends(plugin.session_dependency())
):
    return await db.select_one("SELECT * FROM users WHERE id = $1", user_id)
```

## Starlette

```python
from starlette.applications import Starlette
from starlette.routing import Route
from sqlspec.extensions.starlette import SQLSpecPlugin

spec = SQLSpec()
db = spec.add_config(AsyncpgConfig(connection_config={...}))
plugin = SQLSpecPlugin(spec)

async def homepage(request):
    session = plugin.get_session(request)
    users = await session.execute("SELECT * FROM users").all()
    return JSONResponse({"users": users})

app = Starlette(routes=[Route("/", homepage)])
plugin.init_app(app)
```

## Flask

```python
from flask import Flask, request
from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.extensions.flask import SQLSpecPlugin

app = Flask(__name__)
spec = SQLSpec()
db = spec.add_config(SqliteConfig(connection_config={"database": "app.db"}))
plugin = SQLSpecPlugin(spec)
plugin.init_app(app)

@app.route('/users/<int:user_id>')
def get_user(user_id):
    db = plugin.get_session(request)
    return db.select_one("SELECT * FROM users WHERE id = ?", user_id)
```

## Commit Modes

- **manual**: No automatic transaction management
- **autocommit**: Commit on 2xx, rollback on 4xx/5xx
- **autocommit_include_redirect**: Commit on 2xx and 3xx

## Multi-Database Setup

```python
# Configure multiple databases
primary = spec.add_config(AsyncpgConfig(
    connection_config={"dsn": "postgresql://localhost/main"},
    extension_config={"litestar": {"session_key": "primary_db"}}
))

cache = spec.add_config(SqliteConfig(
    connection_config={"database": "cache.db"},
    extension_config={"litestar": {"session_key": "cache_db"}}
))

# Use via dependency injection
@get("/data")
async def get_data(primary_db: AsyncpgDriver, cache_db: SqliteDriver):
    # Use both databases
    pass
```

## Disabling Built-in DI

For custom DI solutions (Dishka, dependency-injector):

```python
config = AsyncpgConfig(
    connection_config={...},
    extension_config={
        "litestar": {"disable_di": True}
    }
)
# You handle lifecycle manually
```
