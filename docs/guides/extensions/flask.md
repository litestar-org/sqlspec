---
orphan: true
---

# Flask Integration Guide

Explains how to use the SQLSpec Flask extension for automatic request-scoped sessions, transaction management, and multi-database support in Flask applications.

## Quick Facts

- Install with `pip install "sqlspec[sqlite]" flask` (swap the adapter extra for your database).
- Use the `SQLSpecPlugin` for automatic lifecycle management and transaction handling.
- Store database sessions on `flask.g` with automatic cleanup.
- Three commit modes: `manual`, `autocommit`, and `autocommit_include_redirect`.
- Supports both sync and async database adapters (async via portal pattern).
- Multi-database support with unique session keys.

## Installation

```bash
pip install "sqlspec[sqlite]" flask
# or
uv pip install "sqlspec[psycopg]" flask
```

Flask uses WSGI (synchronous), so **synchronous adapters** are recommended for best performance (`SqliteConfig`, `PsycopgConfig` in sync mode, `DuckDBConfig`). Async adapters work via the portal pattern but add ~1-2ms overhead per operation.

## Quick Start

```python
from flask import Flask
from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.extensions.flask import SQLSpecPlugin

sqlspec = SQLSpec()
config = SqliteConfig(
    pool_config={"database": "app.db"},
    extension_config={
        "flask": {
            "commit_mode": "autocommit",
            "session_key": "db"
        }
    }
)
sqlspec.add_config(config)

app = Flask(__name__)
plugin = SQLSpecPlugin(sqlspec, app)

@app.route("/users")
def list_users():
    db = plugin.get_session()
    result = db.execute("SELECT * FROM users")
    return {"users": result.all()}
```

The extension handles connection pooling, request-scoped sessions, and automatic transaction management.

## Configuration Options

Configure the Flask extension via `extension_config["flask"]`:

```python
from sqlspec.config import FlaskConfig

config = SqliteConfig(
    pool_config={"database": "app.db"},
    extension_config={
        "flask": FlaskConfig(
            connection_key="db_connection",  # Optional: Flask g object key for connection
            session_key="db",                # Session key for multi-database setups
            commit_mode="autocommit",        # Transaction handling mode
            extra_commit_statuses={201, 204},     # Additional status codes that commit
            extra_rollback_statuses={409, 422}    # Additional status codes that rollback
        )
    }
)
```

### Configuration Fields

- **`connection_key`** (str, optional): Key for storing connection in Flask `g`. Default: auto-generated from `session_key`.
- **`session_key`** (str, optional): Key for accessing session via `plugin.get_session(key)`. Default: `"default"`.
- **`commit_mode`** (str, optional): Transaction handling mode. Default: `"manual"`.
  - `"manual"`: No automatic transactions, user handles commits explicitly
  - `"autocommit"`: Commits on 2xx status codes, rollback otherwise
  - `"autocommit_include_redirect"`: Commits on 2xx-3xx status codes, rollback otherwise
- **`extra_commit_statuses`** (set[int], optional): Additional HTTP status codes that trigger commit.
- **`extra_rollback_statuses`** (set[int], optional): Additional HTTP status codes that trigger rollback.

## Commit Modes

### Manual Mode (Default)

No automatic transactions. You handle commits and rollbacks explicitly:

```python
from flask import Flask
from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.extensions.flask import SQLSpecPlugin

sqlspec = SQLSpec()
config = SqliteConfig(
    pool_config={"database": "app.db"},
    extension_config={
        "flask": {"commit_mode": "manual", "session_key": "db"}
    }
)
sqlspec.add_config(config)

app = Flask(__name__)
plugin = SQLSpecPlugin(sqlspec, app)

@app.route("/users", methods=["POST"])
def create_user():
    from flask import request

    data = request.get_json()
    db = plugin.get_session()

    db.execute("INSERT INTO users (email) VALUES (?)", (data["email"],))

    # Explicit commit required
    conn = plugin.get_connection()
    conn.commit()

    return {"created": True}, 201
```

**When to use**: Complex transactions spanning multiple operations, manual transaction control needed.

### Autocommit Mode

Automatically commits on successful responses (2xx status codes), rolls back otherwise:

```python
config = SqliteConfig(
    pool_config={"database": "app.db"},
    extension_config={
        "flask": {"commit_mode": "autocommit", "session_key": "db"}
    }
)

@app.route("/users", methods=["POST"])
def create_user():
    data = request.get_json()
    db = plugin.get_session()

    db.execute("INSERT INTO users (email) VALUES (?)", (data["email"],))

    # Auto-commits on 201 response
    return {"created": True}, 201

@app.route("/users/<int:user_id>", methods=["DELETE"])
def delete_user(user_id):
    db = plugin.get_session()

    db.execute("DELETE FROM users WHERE id = ?", (user_id,))

    # Auto-commits on 204 response
    return "", 204

@app.route("/users/bad", methods=["POST"])
def create_bad_user():
    db = plugin.get_session()

    db.execute("INSERT INTO users (email) VALUES (?)", ("invalid",))

    # Auto-rollback on 400 response (not 2xx)
    return {"error": "Invalid data"}, 400
```

**When to use**: Simple CRUD operations, APIs where HTTP status indicates transaction outcome.

### Autocommit Include Redirect Mode

Commits on 2xx and 3xx status codes (including redirects):

```python
config = SqliteConfig(
    pool_config={"database": "app.db"},
    extension_config={
        "flask": {"commit_mode": "autocommit_include_redirect", "session_key": "db"}
    }
)

@app.route("/users", methods=["POST"])
def create_user():
    from flask import redirect

    data = request.get_json()
    db = plugin.get_session()

    result = db.execute(
        "INSERT INTO users (email) VALUES (?) RETURNING id",
        (data["email"],)
    )
    user_id = result.scalar()

    # Auto-commits on 302 redirect
    return redirect(f"/users/{user_id}")
```

**When to use**: Form submissions that redirect on success, traditional web applications.

### Custom Commit/Rollback Status Codes

Add extra status codes for commit/rollback:

```python
config = SqliteConfig(
    pool_config={"database": "app.db"},
    extension_config={
        "flask": {
            "commit_mode": "autocommit",
            "extra_commit_statuses": {202, 204},  # Commit on Accepted, No Content
            "extra_rollback_statuses": {409, 422}  # Rollback on Conflict, Unprocessable
        }
    }
)
```

## Multi-Database Support

Use multiple databases in a single Flask application:

```python
from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.extensions.flask import SQLSpecPlugin

sqlspec = SQLSpec()

# Primary database (user data)
users_config = SqliteConfig(
    pool_config={"database": "users.db"},
    extension_config={
        "flask": {
            "commit_mode": "autocommit",
            "session_key": "users_db"
        }
    }
)

# Analytics database (events)
events_config = DuckDBConfig(
    pool_config={"database": "events.db"},
    extension_config={
        "flask": {
            "commit_mode": "autocommit",
            "session_key": "events_db"
        }
    }
)

sqlspec.add_config(users_config)
sqlspec.add_config(events_config)

app = Flask(__name__)
plugin = SQLSpecPlugin(sqlspec, app)

@app.route("/dashboard")
def dashboard():
    # Access different databases by key
    users_db = plugin.get_session(key="users_db")
    events_db = plugin.get_session(key="events_db")

    user_count = users_db.execute("SELECT COUNT(*) FROM users").scalar()
    event_count = events_db.execute("SELECT COUNT(*) FROM events").scalar()

    return {
        "users": user_count,
        "events": event_count
    }
```

**Key requirements**:
- Each config must have a unique `session_key`
- Access sessions via `plugin.get_session(key="your_key")`
- Each database has independent transaction handling

## Application Factory Pattern

Initialize the extension in a factory function:

```python
from flask import Flask
from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.extensions.flask import SQLSpecPlugin

def create_app(config=None):
    app = Flask(__name__)

    if config:
        app.config.from_object(config)

    # Create SQLSpec and config
    sqlspec = SQLSpec()
    db_config = SqliteConfig(
        pool_config={"database": app.config.get("DATABASE_URL", "app.db")},
        extension_config={
            "flask": {
                "commit_mode": app.config.get("DB_COMMIT_MODE", "autocommit"),
                "session_key": "db"
            }
        }
    )
    sqlspec.add_config(db_config)

    # Initialize extension
    plugin = SQLSpecPlugin(sqlspec)
    plugin.init_app(app)

    # Store plugin on app for route access
    app.sqlspec_plugin = plugin

    # Register blueprints
    from .views import users_bp
    app.register_blueprint(users_bp)

    return app
```

Access the plugin in blueprints:

```python
from flask import Blueprint, current_app

users_bp = Blueprint("users", __name__)

@users_bp.route("/users")
def list_users():
    plugin = current_app.sqlspec_plugin
    db = plugin.get_session()
    result = db.execute("SELECT * FROM users")
    return {"users": result.all()}
```

## Session Caching

Sessions are automatically cached per request for consistency:

```python
@app.route("/test")
def test_session_caching():
    db1 = plugin.get_session()
    db2 = plugin.get_session()

    # Same session instance returned within single request
    assert db1 is db2

    return {"cached": True}
```

This ensures:
- Single database session per request
- Consistent transaction boundaries
- No accidental nested transactions

## Async Adapters (Experimental)

Flask is synchronous (WSGI), but you can use async database adapters via the **portal pattern**. The portal runs a background thread with an event loop to execute async operations:

```python
from sqlspec.adapters.asyncpg import AsyncpgConfig

sqlspec = SQLSpec()
config = AsyncpgConfig(
    pool_config={"dsn": "postgresql://localhost/mydb"},
    extension_config={
        "flask": {"commit_mode": "autocommit", "session_key": "db"}
    }
)
sqlspec.add_config(config)

app = Flask(__name__)
plugin = SQLSpecPlugin(sqlspec, app)  # Portal auto-created for async configs

@app.route("/users")
def list_users():
    # Async operations run via portal (adds ~1-2ms overhead)
    db = plugin.get_session()
    result = db.execute("SELECT * FROM users")
    return {"users": result.all()}
```

**Portal behavior**:
- Automatically created when any async config is registered
- Zero overhead for sync-only configurations
- Thread-safe with queue-based communication
- 30-second timeout prevents deadlocks

**Performance**: Portal adds ~1-2ms per async operation. For best performance, use sync adapters (`SqliteConfig`, `PsycopgConfig` sync mode, `DuckDBConfig`).

**When to use async adapters**:
- Need asyncpg's excellent connection pooling
- Already using async elsewhere in your stack
- Need async-specific driver features

## CLI Commands

Access the database in Flask CLI commands:

```python
import click
from flask.cli import with_appcontext

@click.command("init-db")
@with_appcontext
def init_db_command():
    """Initialize the database schema."""
    plugin = current_app.sqlspec_plugin
    db = plugin.get_session()

    db.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL UNIQUE
        )
    """)

    conn = plugin.get_connection()
    conn.commit()

    click.echo("Database initialized")

app.cli.add_command(init_db_command)
```

Run with: `flask init-db`

## Testing

Use temporary databases for isolation:

```python
import pytest
from your_app import create_app

@pytest.fixture
def app():
    app = create_app({
        "TESTING": True,
        "DATABASE_URL": ":memory:",
        "DB_COMMIT_MODE": "manual"
    })

    with app.app_context():
        # Setup schema
        plugin = app.sqlspec_plugin
        db = plugin.get_session()
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)")
        plugin.get_connection().commit()

    yield app

@pytest.fixture
def client(app):
    return app.test_client()

def test_list_users(client):
    response = client.get("/users")
    assert response.status_code == 200
    assert response.json == {"users": []}
```

## Error Handling

The extension handles connection cleanup automatically, even when exceptions occur:

```python
@app.route("/users/<int:user_id>")
def get_user(user_id):
    db = plugin.get_session()

    result = db.execute("SELECT * FROM users WHERE id = ?", (user_id,))
    user = result.get_first()

    if user is None:
        # Connection still cleaned up on 404
        abort(404)

    return {"user": dict(user)}

@app.errorhandler(Exception)
def handle_exception(e):
    # Connections cleaned up even on unhandled exceptions
    return {"error": "Internal server error"}, 500
```

## Performance Considerations

### Sync vs Async Adapters

**Sync adapters** (recommended for Flask):
- Zero overhead
- Direct database calls
- Examples: `SqliteConfig`, `PsycopgConfig` sync mode, `DuckDBConfig`

**Async adapters** (via portal):
- ~1-2ms overhead per operation
- Background thread with event loop
- Examples: `AsyncpgConfig`, `AiosqliteConfig`, `AsyncmyConfig`

**Recommendation**: Use sync adapters unless you specifically need async features.

### Connection Pooling

Sync adapters support connection pooling:

```python
from sqlspec.adapters.sqlite import SqliteConfig

config = SqliteConfig(
    pool_config={
        "database": "app.db",
        "check_same_thread": False,  # Required for pooling
        "timeout": 30.0
    }
)
```

Pooling reduces connection overhead but requires proper configuration.

## Migration from Manual Pattern

**Before** (manual context manager pattern):

```python
from flask import g

def get_session():
    if "db_session" not in g:
        g.db_session = spec.provide_session(db_config).__enter__()
    return g.db_session

@app.teardown_appcontext
def close_session(exception):
    session = g.pop("db_session", None)
    if session is not None:
        session.__exit__(None, None, None)

@app.route("/users")
def list_users():
    session = get_session()
    result = session.execute("SELECT * FROM users")
    return {"users": result.all()}
```

**After** (extension pattern):

```python
from sqlspec.extensions.flask import SQLSpecPlugin

sqlspec = SQLSpec()
config = SqliteConfig(
    pool_config={"database": "app.db"},
    extension_config={"flask": {"commit_mode": "autocommit"}}
)
sqlspec.add_config(config)

plugin = SQLSpecPlugin(sqlspec, app)

@app.route("/users")
def list_users():
    db = plugin.get_session()
    result = db.execute("SELECT * FROM users")
    return {"users": result.all()}
```

**Benefits**:
- Less boilerplate code
- Automatic lifecycle management
- Built-in transaction handling
- Multi-database support
- Consistent API across frameworks

## Troubleshooting

### "SQLSpec extension already registered"

**Cause**: Called `init_app()` twice on the same Flask app.

**Solution**: Only call `init_app()` once, or use application factory pattern:

```python
# Good
plugin = SQLSpecPlugin(sqlspec)
plugin.init_app(app)

# Bad
SQLSpecPlugin(sqlspec, app)  # Calls init_app
plugin.init_app(app)          # Error: already registered
```

### "Duplicate state keys found"

**Cause**: Multiple configs with the same `session_key`.

**Solution**: Ensure each config has a unique `session_key`:

```python
config1 = SqliteConfig(
    pool_config={"database": "db1.db"},
    extension_config={"flask": {"session_key": "db1"}}
)
config2 = SqliteConfig(
    pool_config={"database": "db2.db"},
    extension_config={"flask": {"session_key": "db2"}}  # Must be unique!
)
```

### Sessions not committing in autocommit mode

**Cause**: Route returns non-2xx status code.

**Solution**:
1. Check your route returns 200-299 status code
2. Or use `extra_commit_statuses` to commit on other status codes
3. Or use `manual` mode and commit explicitly

### Portal timeout errors (async adapters)

**Cause**: Async operation took longer than 30 seconds.

**Solution**:
1. Optimize slow queries
2. Consider using sync adapters for better performance
3. Portal has hardcoded 30-second timeout - use async frameworks (Quart, FastAPI) for true async support

## Best Practices

1. **Use sync adapters** for Flask (WSGI is synchronous)
2. **Choose commit mode** based on your use case:
   - `autocommit` for simple CRUD APIs
   - `manual` for complex transactions
3. **Unique session keys** for multi-database setups
4. **Application factory pattern** for testability
5. **Temporary databases** for integration tests
6. **Monitor connection pools** via health check endpoints
7. **Keep routes simple** - move complex logic to service layer

## See Also

- [Starlette Integration Guide](starlette.md) - Async ASGI framework
- [FastAPI Integration Guide](fastapi.md) - FastAPI-specific dependency injection
- [Litestar Integration Guide](litestar.md) - Full-featured async framework
- [Testing Guide](../testing/testing.md) - Comprehensive testing patterns
