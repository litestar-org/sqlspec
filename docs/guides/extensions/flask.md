---
orphan: true
---

# Flask Integration Guide

Explains how to combine SQLSpec with Flask applications using request-scoped sessions, CLI commands, and transaction helpers for synchronous drivers.

## Quick Facts

- Install with `pip install "sqlspec[sqlite]" flask` (swap the adapter extra for your database).
- Use synchronous adapters (`SqliteConfig`, `PsycopgConfig`, `DuckDBConfig`) for compatibility with Flask’s WSGI request cycle.
- Store the SQLSpec session on `flask.g` and release it in `teardown_appcontext`.
- Wrap multi-statement operations in explicit transactions via `session.begin()` / `session.commit()`.
- Access connection pools through `spec.get_pool(config)` when you need status endpoints or health checks.
- Reuse SQLSpec sessions inside CLI commands, Celery tasks, and background jobs.

## Installation

```bash
pip install "sqlspec[sqlite]" flask
# or
uv pip install "sqlspec[psycopg]" flask
```

Flask does not provide async views, so choose a synchronous adapter. To run async adapters, execute them inside asyncio-compatible workers such as Quart instead of Flask.

## Application Setup

```python
from flask import Flask, g, jsonify, request
from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.driver import SyncDriverAdapterBase

spec = SQLSpec()
db_config = spec.add_config(SqliteConfig(pool_config={"database": "app.db"}))

app = Flask(__name__)


def get_session() -> SyncDriverAdapterBase:
    if "db_session" not in g:
        g.db_session = spec.provide_session(db_config).__enter__()
    return g.db_session


@app.teardown_appcontext
def close_session(exception: Exception | None) -> None:
    session = g.pop("db_session", None)
    if session is not None:
        if exception is None:
            session.__exit__(None, None, None)
        else:
            session.__exit__(exception.__class__, exception, exception.__traceback__)
```

`provide_session()` returns a context manager. Manually enter and exit it so every request receives a fresh session.

## Route Handlers

```python
@app.route("/users/<int:user_id>", methods=["GET"])
def read_user(user_id: int):
    session = get_session()
    result = session.execute("SELECT id, email FROM users WHERE id = ?", user_id)
    return jsonify(result.one())


@app.route("/users", methods=["POST"])
def create_user():
    payload = request.get_json()
    session = get_session()
    session.begin()
    try:
        result = session.execute(
            "INSERT INTO users (email) VALUES (?) RETURNING id",
            payload["email"],
        )
    except Exception:
        session.rollback()
        raise
    else:
        session.commit()
        return jsonify({"id": result.scalar(), "email": payload["email"]}), 201
```

Use parameter placeholders appropriate for the adapter (`?` for SQLite, `%s` for psycopg in sync mode). SQLSpec handles conversions automatically.

## CLI Commands

Register database commands with Flask’s CLI:

```python
import click
from flask.cli import with_appcontext


@click.command("init-db")
@with_appcontext
def init_db() -> None:
    session = spec.provide_session(db_config).__enter__()
    try:
        session.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT NOT NULL UNIQUE
            )
            """
        )
    finally:
        session.__exit__(None, None, None)


app.cli.add_command(init_db)
```

CLI commands share the same configuration and benefit from SQLSpec’s parameter style conversions.

## Background Jobs

When running background jobs (Celery, RQ, APScheduler), open a new session inside the job:

```python
def send_digest() -> None:
    session = spec.provide_session(db_config).__enter__()
    try:
        rows = session.execute("SELECT email FROM users").all()
        # send emails...
    finally:
        session.__exit__(None, None, None)
```

Avoid sharing sessions between threads or processes; SQLSpec sessions are not thread-safe.

## Testing Strategy

- Use Flask’s `app.test_client()` with a temporary database (e.g., `":memory:"` for SQLite).
- Create a pytest fixture that sets up the schema using SQLSpec sessions and tears it down after tests.
- Stub out `get_session()` to point at the fixture-managed session when writing isolated unit tests.

## Operational Notes

- Monitor pool utilization via adapter-specific APIs (`spec.get_pool(db_config)`) to expose metrics endpoints.
- Handle migrations outside the request cycle using Alembic or SQLSpec’s migration tools.
- Keep commit logic deterministic: for read-only requests, skip explicit calls to `begin()` / `commit()` to avoid unnecessary locking.
- Wrap session entry/exit in try/finally blocks when integrating with libraries that bypass Flask’s request lifecycle.
